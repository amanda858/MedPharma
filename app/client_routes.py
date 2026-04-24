"""Client Hub API — auth, claims queue, payments, notes, credentialing, enrollment, EDI, providers, dashboard."""

import os
import json as _json
import re
import logging
import shutil
import sqlite3
import threading
import uuid
from datetime import datetime, date, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Cookie, Response, Request, UploadFile, File as FastAPIFile, Form, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

IS_PROD = bool(os.getenv("PORT"))  # Render sets PORT in production

from app.client_db import (
    get_db,
    authenticate, validate_session, logout_session,
    list_clients, create_client, update_client, delete_client,
    get_profile, update_profile,
    get_practice_profiles, upsert_practice_profile, delete_practice_profile,
    list_providers, create_provider, update_provider, delete_provider,
    get_claims, get_claim, create_claim, update_claim, delete_claim,
    get_payments, create_payment, delete_payment,
    get_notes, add_note,
    get_credentialing, create_credentialing, update_credentialing, delete_credentialing,
    get_enrollment, create_enrollment, update_enrollment, delete_enrollment,
    get_edi, create_edi, update_edi, delete_edi,
    get_dashboard, CLAIM_STATUSES,
    list_files, add_file, get_file_record, update_file_record, delete_file_record,
    list_production_logs, add_production_log, delete_production_log, get_production_report,
    log_audit, get_audit_log, auto_flag_sla, get_alerts,
    global_search, bulk_update_claims, export_claims, export_table,
    get_report_notes, upsert_report_note, delete_report_note, rename_report_note,
)

from app.notifications import (
    notify_activity,
    notify_bulk_activity,
    flush_and_notify,
    send_test_notification,
    get_notification_status,
)
from rule_intercept import intercept_excel_upload

router = APIRouter(prefix="/hub/api")


DATA_IMPORT_CATEGORIES = ("Claims", "Credentialing", "Enrollment", "EDI")


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _infer_excel_category(content: bytes, ext: str, filename: str = "", description: str = "") -> tuple[Optional[str], dict]:
    """Infer the best import category from Excel headers + filename/description text."""
    scores = {"Claims": 0, "Credentialing": 0, "Enrollment": 0, "EDI": 0}
    keywords = {
        "Claims": [
            "claim", "patient", "dos", "cpt", "charge", "allowed", "paid", "balance", "denial", "remit",
        ],
        "Credentialing": [
            "credential", "recredential", "expiration", "approved", "follow up", "provider enrollment",
        ],
        "Enrollment": [
            "enroll", "effective", "participation", "in network", "in-network", "payer enrollment",
        ],
        "EDI": [
            "edi", "era", "eft", "clearinghouse", "trading partner", "submitter", "receiver", "payer id", "837", "835",
        ],
    }

    headers: list[str] = []
    try:
        rows = _parse_excel_rows(content, ext, combine_sheets=True)
        if rows:
            headers = [str(k or "") for k in rows[0].keys()]
    except Exception:
        headers = []

    blob = " ".join([*headers, filename or "", description or ""]).lower()

    # Rule-intercept (deterministic) gets first shot.
    intercept = intercept_excel_upload(headers=headers, filename=filename, description=description)
    intercepted_category = intercept.get("category")
    if intercepted_category in DATA_IMPORT_CATEGORIES:
        debug = {
            "scores": scores,
            "headers_sample": headers[:20],
            "best_score": None,
            "second_score": None,
            "intercept": intercept,
        }
        return intercepted_category, debug

    # Heuristic fallback.
    for category, words in keywords.items():
        for word in words:
            if word in blob:
                # Longer keywords get a little more weight to reduce collisions.
                scores[category] += 2 if len(word) >= 7 else 1

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_cat, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0
    inferred = best_cat if best_score >= 2 and (best_score - second_score) >= 1 else None
    debug = {
        "scores": scores,
        "headers_sample": headers[:20],
        "best_score": best_score,
        "second_score": second_score,
        "intercept": intercept,
    }
    return inferred, debug


# ─── Auth helpers ─────────────────────────────────────────────────────────────

def _get_user(hub_session: Optional[str] = None):
    """Return the authenticated user dict, or None if not logged in."""
    if not hub_session:
        return None
    return validate_session(hub_session)


def _require_user(hub_session: Optional[str] = Cookie(None)):
    """Return the authenticated user or raise 401."""
    user = _get_user(hub_session)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def _require_admin(hub_session: Optional[str] = Cookie(None)):
    """Return the authenticated admin user or raise 401/403."""
    user = _require_user(hub_session)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _client_scope(user: dict) -> Optional[int]:
    """Return client_id filter — None means all (admin sees all data)."""
    if user.get("role") == "admin":
        return None
    return user["id"]


# ─── Auth ─────────────────────────────────────────────────────────────────────

class LoginIn(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(body: LoginIn, response: Response):
    user, token = authenticate(body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    response.set_cookie(
        "hub_session", token,
        httponly=True,
        samesite="lax",
        secure=IS_PROD,        # Only require HTTPS in production (Render)
        path="/",              # Explicit root path — available to ALL routes
        max_age=86400 * 30,
    )
    return {"ok": True, "user": user}


@router.post("/logout")
def logout(response: Response, hub_session: Optional[str] = Cookie(None)):
    # Capture user info BEFORE deleting session
    user = _get_user(hub_session) if hub_session else None
    # Always delete session + cookie first — this must succeed unconditionally
    if hub_session:
        try:
            logout_session(hub_session)
        except Exception as exc:
            log.error(f"logout_session error (continuing): {exc}")
    response.delete_cookie("hub_session", path="/")
    # Fire progress report in a background thread — non-blocking, non-critical
    if user:
        threading.Thread(
            target=flush_and_notify,
            args=(user["username"],),
            daemon=True,
        ).start()
    return {"ok": True}


@router.get("/me")
def me(hub_session: Optional[str] = Cookie(None)):
    user = _get_user(hub_session)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


# ─── Accounts (for selector screen) ──────────────────────────────────────────

@router.get("/accounts")
def accounts(hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)

    # Account selector should show client companies only (not internal/admin users),
    # and avoid duplicate cards for the same company.
    clients = [
        c for c in list_clients()
        if c.get("role") == "client" and int(c.get("is_active", 0) or 0) == 1
    ]

    deduped: dict[str, dict] = {}
    for c in clients:
        key = str(c.get("company") or "").strip().lower()
        if not key:
            key = f"id:{c.get('id')}"
        prev = deduped.get(key)
        if prev is None or int(c.get("id", 0) or 0) < int(prev.get("id", 0) or 0):
            deduped[key] = c

    return sorted(deduped.values(), key=lambda x: str(x.get("company") or "").lower())


# ─── Clients (admin) ──────────────────────────────────────────────────────────

class ClientIn(BaseModel):
    username: str
    password: str
    company: str
    contact_name: Optional[str] = ""
    email: Optional[str] = ""
    phone: Optional[str] = ""
    role: Optional[str] = "client"


class ClientUpdate(BaseModel):
    company: Optional[str] = None
    contact_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[int] = None
    password: Optional[str] = None


class ProfileUpdate(BaseModel):
    company: Optional[str] = None
    contact_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    tax_id: Optional[str] = None
    group_npi: Optional[str] = None
    individual_npi: Optional[str] = None
    ptan_group: Optional[str] = None
    ptan_individual: Optional[str] = None
    address: Optional[str] = None
    specialty: Optional[str] = None
    notes: Optional[str] = None
    practice_type: Optional[str] = None
    doc_tabs: Optional[list] = None
    report_tabs: Optional[list] = None


class PracticeProfileUpdate(BaseModel):
    practice_type: Optional[str] = None
    specialty: Optional[str] = None
    tax_id: Optional[str] = None
    group_npi: Optional[str] = None
    individual_npi: Optional[str] = None
    ptan_group: Optional[str] = None
    ptan_individual: Optional[str] = None
    address: Optional[str] = None
    contact_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    notes: Optional[str] = None


@router.get("/clients")
def get_clients(hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    return list_clients()


@router.post("/clients")
def add_client(body: ClientIn, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    cid = create_client(body.model_dump())
    return {"id": cid, "ok": True}


@router.put("/clients/{cid}")
def edit_client(cid: int, body: ClientUpdate, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    update_client(cid, {k: v for k, v in body.model_dump().items() if v is not None})
    return {"ok": True}


@router.delete("/clients/{cid}")
def remove_client(cid: int, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    delete_client(cid)
    return {"ok": True}


# ─── Profile (own client profile) ──────────────────────────────────────────────────────────

@router.get("/profile")
def get_my_profile(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    return get_profile(cid)


@router.get("/profile/{cid}")
def get_client_profile(cid: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    return get_profile(cid)


@router.put("/profile")
def update_my_profile(body: ProfileUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    data = {k: v for k, v in body.model_dump().items() if v is not None and k not in ("doc_tabs", "report_tabs")}
    if body.doc_tabs is not None:
        data["doc_tab_names"] = _json.dumps(body.doc_tabs)
    if body.report_tabs is not None:
        data["report_tab_names"] = _json.dumps(body.report_tabs)
    update_profile(cid, data)
    return {"ok": True}


@router.put("/profile/{cid}")
def update_client_profile(cid: int, body: ProfileUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    # Any authenticated user can edit any client profile
    data = {k: v for k, v in body.model_dump().items() if v is not None and k not in ("doc_tabs", "report_tabs")}
    if body.doc_tabs is not None:
        data["doc_tab_names"] = _json.dumps(body.doc_tabs)
    if body.report_tabs is not None:
        data["report_tab_names"] = _json.dumps(body.report_tabs)
    update_profile(cid, data)
    return {"ok": True}


# ─── Report Notes (custom report tab content) ──────────────────────────────────

class ReportNoteBody(BaseModel):
    tab_name: str
    content: str = ""

class ReportNoteRenameBody(BaseModel):
    old_name: str
    new_name: str

@router.get("/report-notes/{cid}")
def get_client_report_notes(cid: int, tab_name: Optional[str] = None,
                             hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    notes = get_report_notes(cid, tab_name)
    return {"notes": notes}

@router.put("/report-notes/{cid}")
def save_report_note(cid: int, body: ReportNoteBody,
                     hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    upsert_report_note(cid, body.tab_name, body.content, user.get("username", ""))
    return {"ok": True}

@router.delete("/report-notes/{cid}/{tab_name}")
def remove_report_note(cid: int, tab_name: str,
                       hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    delete_report_note(cid, tab_name)
    return {"ok": True}

@router.put("/report-notes/{cid}/rename")
def rename_report_note_endpoint(cid: int, body: ReportNoteRenameBody,
                                hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    rename_report_note(cid, body.old_name, body.new_name)
    return {"ok": True}


# ─── Practice Sub-Profiles ─────────────────────────────────────────────────────────────

@router.get("/practice-profiles")
def list_practice_profiles(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    return {"profiles": get_practice_profiles(cid)}


@router.get("/practice-profiles/{cid}")
def list_practice_profiles_admin(cid: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    return {"profiles": get_practice_profiles(cid)}


@router.put("/practice-profiles/{profile_name}")
def save_practice_profile(profile_name: str, body: PracticeProfileUpdate,
                          hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    upsert_practice_profile(cid, profile_name, body.model_dump(exclude_none=True))
    return {"ok": True}


@router.put("/practice-profiles/{cid}/{profile_name}")
def save_practice_profile_admin(cid: int, profile_name: str, body: PracticeProfileUpdate,
                                hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    upsert_practice_profile(cid, profile_name, body.model_dump(exclude_none=True))
    return {"ok": True}


@router.delete("/practice-profiles/{pp_id}")
def remove_practice_profile(pp_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    delete_practice_profile(pp_id, cid)
    return {"ok": True}


# ─── Providers ────────────────────────────────────────────────────────────────

class ProviderIn(BaseModel):
    client_id: int
    ProviderName: str
    NPI: Optional[str] = ""
    Specialty: Optional[str] = ""
    TaxID: Optional[str] = ""
    Email: Optional[str] = ""
    Phone: Optional[str] = ""
    Status: Optional[str] = "Active"
    StartDate: Optional[str] = ""
    Notes: Optional[str] = ""
    sub_profile: Optional[str] = ""


class ProviderUpdate(BaseModel):
    ProviderName: Optional[str] = None
    NPI: Optional[str] = None
    Specialty: Optional[str] = None
    TaxID: Optional[str] = None
    Email: Optional[str] = None
    Phone: Optional[str] = None
    Status: Optional[str] = None
    StartDate: Optional[str] = None
    Notes: Optional[str] = None
    sub_profile: Optional[str] = None


@router.get("/providers")
def get_providers(client_id: Optional[int] = None, sub_profile: Optional[str] = None,
                 hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    return list_providers(scope, sub_profile=sub_profile)


@router.post("/providers")
def add_provider(body: ProviderIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = body.model_dump()
    if user["role"] != "admin":
        data["client_id"] = user["id"]
    pid = create_provider(data)
    return {"id": pid, "ok": True}


@router.put("/providers/{pid}")
def edit_provider(pid: int, body: ProviderUpdate, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    update_provider(pid, {k: v for k, v in body.model_dump().items() if v is not None})
    return {"ok": True}


@router.delete("/providers/{pid}")
def remove_provider(pid: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    delete_provider(pid)
    return {"ok": True}


# ─── Claims ───────────────────────────────────────────────────────────────────

class ClaimIn(BaseModel):
    client_id: int
    ClaimKey: str
    PatientID: Optional[str] = ""
    PatientName: Optional[str] = ""
    Payor: Optional[str] = ""
    ProviderName: Optional[str] = ""
    NPI: Optional[str] = ""
    DOS: Optional[str] = ""
    CPTCode: Optional[str] = ""
    Description: Optional[str] = ""
    ChargeAmount: Optional[float] = 0
    AllowedAmount: Optional[float] = 0
    AdjustmentAmount: Optional[float] = 0
    PaidAmount: Optional[float] = 0
    BalanceRemaining: Optional[float] = 0
    ClaimStatus: Optional[str] = "Intake"
    BillDate: Optional[str] = ""
    DeniedDate: Optional[str] = ""
    PaidDate: Optional[str] = ""
    Owner: Optional[str] = ""
    NextAction: Optional[str] = ""
    NextActionDueDate: Optional[str] = ""
    SLABreached: Optional[int] = 0
    DenialCategory: Optional[str] = ""
    DenialReason: Optional[str] = ""
    AppealDate: Optional[str] = ""
    AppealStatus: Optional[str] = ""
    sub_profile: Optional[str] = ""


class ClaimUpdate(BaseModel):
    PatientID: Optional[str] = None
    PatientName: Optional[str] = None
    Payor: Optional[str] = None
    ProviderName: Optional[str] = None
    NPI: Optional[str] = None
    DOS: Optional[str] = None
    CPTCode: Optional[str] = None
    Description: Optional[str] = None
    ChargeAmount: Optional[float] = None
    AllowedAmount: Optional[float] = None
    AdjustmentAmount: Optional[float] = None
    PaidAmount: Optional[float] = None
    BalanceRemaining: Optional[float] = None
    ClaimStatus: Optional[str] = None
    BillDate: Optional[str] = None
    DeniedDate: Optional[str] = None
    PaidDate: Optional[str] = None
    Owner: Optional[str] = None
    NextAction: Optional[str] = None
    NextActionDueDate: Optional[str] = None
    SLABreached: Optional[int] = None
    DenialCategory: Optional[str] = None
    DenialReason: Optional[str] = None
    AppealDate: Optional[str] = None
    AppealStatus: Optional[str] = None
    sub_profile: Optional[str] = None


@router.get("/claims")
def get_claims_list(status: Optional[str] = None, client_id: Optional[int] = None,
                   sub_profile: Optional[str] = None,
                   hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    return {"claims": get_claims(scope, status, sub_profile=sub_profile)}


@router.get("/claims/statuses")
def claim_statuses():
    return CLAIM_STATUSES


@router.get("/claims/{claim_id}")
def get_single_claim(claim_id: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    c = get_claim(claim_id)
    if not c:
        raise HTTPException(404, "Claim not found")
    return c


@router.post("/claims")
def add_claim(body: ClaimIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = body.model_dump()
    if user["role"] != "admin":
        data["client_id"] = user["id"]
    cid = create_claim(data)
    notify_activity(user["username"], "created", "Claims",
                    f"Patient: {data.get('PatientName','')}, Payor: {data.get('Payor','')}")
    return {"id": cid, "ok": True}


@router.put("/claims/{claim_id}")
def edit_claim(claim_id: int, body: ClaimUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    update_claim(claim_id, changes)
    notify_activity(user["username"], "updated", "Claims",
                    f"Claim #{claim_id}, fields: {', '.join(changes.keys())}")
    return {"ok": True}


@router.delete("/claims/{claim_id}")
def remove_claim(claim_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    delete_claim(claim_id)
    notify_activity(user["username"], "deleted", "Claims", f"Claim #{claim_id}")
    return {"ok": True}


# ─── Payments ─────────────────────────────────────────────────────────────────

class PaymentIn(BaseModel):
    ClaimKey: str
    PostDate: Optional[str] = ""
    PaymentAmount: Optional[float] = 0
    AdjustmentAmount: Optional[float] = 0
    PayerType: Optional[str] = "Primary"
    CheckNumber: Optional[str] = ""
    ERA: Optional[str] = ""
    Notes: Optional[str] = ""


@router.get("/claims/{claim_key}/payments")
def list_payments(claim_key: str, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    return get_payments(cid, claim_key)


@router.post("/claims/{claim_key}/payments")
def add_payment(claim_key: str, body: PaymentIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    data = body.model_dump()
    data["client_id"] = cid
    data["ClaimKey"] = claim_key
    pid = create_payment(data)
    return {"id": pid, "ok": True}


@router.delete("/payments/{payment_id}")
def remove_payment(payment_id: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    delete_payment(payment_id)
    return {"ok": True}


# ─── Notes ────────────────────────────────────────────────────────────────────

class NoteIn(BaseModel):
    ClaimKey: Optional[str] = ""
    Module: Optional[str] = "Claim"
    RefID: Optional[int] = 0
    Note: str
    Author: Optional[str] = ""


@router.get("/notes")
def list_notes(claim_key: Optional[str] = None, module: Optional[str] = None,
               ref_id: Optional[int] = None, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    return get_notes(cid, claim_key, module, ref_id)


@router.post("/notes")
def post_note(body: NoteIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    cid = scope if scope is not None else user["id"]
    data = body.model_dump()
    data["client_id"] = cid
    if not data.get("Author"):
        data["Author"] = user.get("username", "")
    nid = add_note(data)
    return {"id": nid, "ok": True}


# ─── Credentialing ────────────────────────────────────────────────────────────

class CredIn(BaseModel):
    client_id: int
    provider_id: Optional[int] = None
    ProviderName: Optional[str] = ""
    Payor: Optional[str] = ""
    CredType: Optional[str] = "Initial"
    Status: Optional[str] = "Not Started"
    SubmittedDate: Optional[str] = ""
    FollowUpDate: Optional[str] = ""
    ApprovedDate: Optional[str] = ""
    ExpirationDate: Optional[str] = ""
    Owner: Optional[str] = ""
    Notes: Optional[str] = ""
    sub_profile: Optional[str] = ""


class CredUpdate(BaseModel):
    ProviderName: Optional[str] = None
    Payor: Optional[str] = None
    CredType: Optional[str] = None
    Status: Optional[str] = None
    SubmittedDate: Optional[str] = None
    FollowUpDate: Optional[str] = None
    ApprovedDate: Optional[str] = None
    ExpirationDate: Optional[str] = None
    Owner: Optional[str] = None
    Notes: Optional[str] = None
    sub_profile: Optional[str] = None


@router.get("/credentialing")
def list_cred(status: Optional[str] = None, client_id: Optional[int] = None,
             sub_profile: Optional[str] = None,
             hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    return get_credentialing(scope, status, sub_profile=sub_profile)


@router.post("/credentialing")
def add_cred(body: CredIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = body.model_dump()
    if user["role"] != "admin":
        data["client_id"] = user["id"]
    rid = create_credentialing(data)
    notify_activity(user["username"], "created", "Credentialing",
                    f"Provider: {data.get('ProviderName','')}, Payor: {data.get('Payor','')}")
    return {"id": rid, "ok": True}


@router.put("/credentialing/{rid}")
def edit_cred(rid: int, body: CredUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    update_credentialing(rid, changes)
    notify_activity(user["username"], "updated", "Credentialing",
                    f"Record #{rid}, fields: {', '.join(changes.keys())}")
    return {"ok": True}


@router.delete("/credentialing/{rid}")
def remove_cred(rid: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    delete_credentialing(rid)
    notify_activity(user["username"], "deleted", "Credentialing", f"Record #{rid}")
    return {"ok": True}


# ─── Enrollment ───────────────────────────────────────────────────────────────

class EnrollIn(BaseModel):
    client_id: int
    provider_id: Optional[int] = None
    ProviderName: Optional[str] = ""
    Payor: Optional[str] = ""
    EnrollType: Optional[str] = "Enrollment"
    Status: Optional[str] = "Not Started"
    SubmittedDate: Optional[str] = ""
    FollowUpDate: Optional[str] = ""
    ApprovedDate: Optional[str] = ""
    EffectiveDate: Optional[str] = ""
    Owner: Optional[str] = ""
    Notes: Optional[str] = ""
    sub_profile: Optional[str] = ""


class EnrollUpdate(BaseModel):
    ProviderName: Optional[str] = None
    Payor: Optional[str] = None
    EnrollType: Optional[str] = None
    Status: Optional[str] = None
    SubmittedDate: Optional[str] = None
    FollowUpDate: Optional[str] = None
    ApprovedDate: Optional[str] = None
    EffectiveDate: Optional[str] = None
    Owner: Optional[str] = None
    Notes: Optional[str] = None
    sub_profile: Optional[str] = None


@router.get("/enrollment")
def list_enroll(status: Optional[str] = None, client_id: Optional[int] = None,
               sub_profile: Optional[str] = None,
               hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    return get_enrollment(scope, status, sub_profile=sub_profile)


@router.post("/enrollment")
def add_enroll(body: EnrollIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = body.model_dump()
    if user["role"] != "admin":
        data["client_id"] = user["id"]
    eid = create_enrollment(data)
    notify_activity(user["username"], "created", "Enrollment",
                    f"Provider: {data.get('ProviderName','')}, Payor: {data.get('Payor','')}")
    return {"id": eid, "ok": True}


@router.put("/enrollment/{rid}")
def edit_enroll(rid: int, body: EnrollUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    update_enrollment(rid, changes)
    notify_activity(user["username"], "updated", "Enrollment",
                    f"Record #{rid}, fields: {', '.join(changes.keys())}")
    return {"ok": True}


@router.delete("/enrollment/{rid}")
def remove_enroll(rid: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    delete_enrollment(rid)
    notify_activity(user["username"], "deleted", "Enrollment", f"Record #{rid}")
    return {"ok": True}


# ─── EDI Setup ────────────────────────────────────────────────────────────────

class EDIIn(BaseModel):
    client_id: int
    provider_id: Optional[int] = None
    ProviderName: Optional[str] = ""
    Payor: Optional[str] = ""
    EDIStatus: Optional[str] = "Not Started"
    ERAStatus: Optional[str] = "Not Started"
    EFTStatus: Optional[str] = "Not Started"
    SubmittedDate: Optional[str] = ""
    GoLiveDate: Optional[str] = ""
    PayerID: Optional[str] = ""
    Owner: Optional[str] = ""
    Notes: Optional[str] = ""
    sub_profile: Optional[str] = ""


class EDIUpdate(BaseModel):
    ProviderName: Optional[str] = None
    Payor: Optional[str] = None
    EDIStatus: Optional[str] = None
    ERAStatus: Optional[str] = None
    EFTStatus: Optional[str] = None
    SubmittedDate: Optional[str] = None
    GoLiveDate: Optional[str] = None
    PayerID: Optional[str] = None
    Owner: Optional[str] = None
    Notes: Optional[str] = None
    sub_profile: Optional[str] = None


@router.get("/edi")
def list_edi(client_id: Optional[int] = None, sub_profile: Optional[str] = None,
            hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    return get_edi(scope, sub_profile=sub_profile)


@router.post("/edi")
def add_edi(body: EDIIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = body.model_dump()
    if user["role"] != "admin":
        data["client_id"] = user["id"]
    eid = create_edi(data)
    notify_activity(user["username"], "created", "EDI Setup",
                    f"Provider: {data.get('ProviderName','')}, Payor: {data.get('Payor','')}")
    return {"id": eid, "ok": True}


@router.put("/edi/{rid}")
def edit_edi(rid: int, body: EDIUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    update_edi(rid, changes)
    notify_activity(user["username"], "updated", "EDI Setup",
                    f"Record #{rid}, fields: {', '.join(changes.keys())}")
    return {"ok": True}


@router.delete("/edi/{rid}")
def remove_edi(rid: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    delete_edi(rid)
    notify_activity(user["username"], "deleted", "EDI Setup", f"Record #{rid}")
    return {"ok": True}


# ─── Dashboard ────────────────────────────────────────────────────────────────

@router.get("/dashboard")
def dashboard(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = get_dashboard(_client_scope(user))
    data["user"] = user
    return data


@router.get("/dashboard/client/{client_id}")
def dashboard_for_client(client_id: int, sub_profile: Optional[str] = None,
                        hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    data = get_dashboard(client_id, sub_profile=sub_profile)
    data["user"] = user
    return data


# ─── File Uploads ───────────────────────────────────────────────────────────

UPLOAD_DIR = os.path.join("data", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ─── Team Production ──────────────────────────────────────────────────────────

class ProductionLogIn(BaseModel):
    client_id: Optional[int] = None
    work_date: str
    category: str = ""
    task_description: str = ""
    quantity: int = 0
    time_spent: float = 0
    notes: str = ""


class ProductionRelinkIn(BaseModel):
    source_client_ids: Optional[list[int]] = None
    usernames: Optional[list[str]] = None
    dry_run: bool = False
    max_rows: int = 5000


@router.get("/production")
def get_production(client_id: Optional[int] = None,
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None,
                   hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    logs = list_production_logs(scope, start_date, end_date, username=None)
    # Turnkey admin fallback: when a selected account has no rows,
    # return all production rows so the panel is never empty by mistake.
    if user.get("role") == "admin" and client_id is not None and not logs:
        logs = list_production_logs(None, start_date, end_date, username=None)
        return {
            "logs": logs,
            "fallback_all_clients": True,
            "selected_client_id": client_id,
        }
    return {"logs": logs, "fallback_all_clients": False, "selected_client_id": client_id}


@router.post("/production")
def create_production_log(body: ProductionLogIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = body.client_id if body.client_id is not None else (_client_scope(user) if _client_scope(user) is not None else user["id"])
    data = body.model_dump()
    data["client_id"] = scope
    data["username"] = user["username"]
    log_id = add_production_log(data)
    notify_activity(user["username"], "logged production", "Time Tracking",
                    f"{data.get('hours',0)}h — {data.get('task_type','')}: {data.get('description','')[:60]}")
    return {"id": log_id, "ok": True}


@router.delete("/production/{log_id}")
def remove_production_log(log_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    delete_production_log(log_id)
    notify_activity(user["username"], "deleted", "Time Tracking", f"Log #{log_id}")
    return {"ok": True}


@router.post("/production/import")
async def import_production_excel(
    client_id: Optional[int] = Query(None),
    file: UploadFile = FastAPIFile(...),
    hub_session: Optional[str] = Cookie(None),
):
    """Import production log entries from an Excel / CSV file."""
    user = _require_user(hub_session)
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")

    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in {"xlsx", "xls", "csv"}:
        raise HTTPException(status_code=422, detail="File must be .xlsx, .xls, or .csv")

    content = await file.read()
    try:
        rows = _parse_excel_rows(content, f".{ext}")
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {exc}")
    if not rows:
        raise HTTPException(status_code=422, detail="No data rows found in file")

    # ── Tiered column finder: exact > starts-with > contains ────────────────
    def _find_col(headers: list[str], *candidates: str) -> Optional[str]:
        hl = [h.lower().strip() for h in headers]
        # Tier 1: exact match
        for c in candidates:
            cl = c.lower()
            for i, h in enumerate(hl):
                if h == cl:
                    return headers[i]
        # Tier 2: starts with
        for c in candidates:
            cl = c.lower()
            for i, h in enumerate(hl):
                if h.startswith(cl):
                    return headers[i]
        # Tier 3: contains
        for c in candidates:
            cl = c.lower()
            for i, h in enumerate(hl):
                if cl in h:
                    return headers[i]
        return None

    headers = list(rows[0].keys()) if rows else []
    # Specific candidates first — broad/conflicting keywords removed
    col_date     = _find_col(headers, "work date", "work_date", "date", "day")
    col_username = _find_col(headers, "username", "user name", "user", "agent", "rep", "employee", "staff", "technician", "tech")
    col_category = _find_col(headers, "category", "task type", "activity type", "work type", "type")
    col_task     = _find_col(headers, "task description", "task", "description", "work performed", "work done", "activity", "detail")
    col_qty      = _find_col(headers, "quantity", "qty", "count", "units", "items")
    col_hours    = _find_col(headers, "hours", "time spent", "duration", "hrs")
    col_notes    = _find_col(headers, "notes", "comments", "comment", "additional", "remarks")

    # ── Conflict resolution: prevent two fields grabbing the same column ────
    used: set[str] = set()
    def _claim(col: Optional[str]) -> Optional[str]:
        if col is None or col in used:
            return None
        used.add(col)
        return col

    col_date     = _claim(col_date)
    col_task     = _claim(col_task)
    # Remaining optional columns — skip if already claimed
    col_username = col_username if col_username not in used else None
    col_category = col_category if col_category not in used else None
    col_qty      = col_qty      if col_qty      not in used else None
    col_hours    = col_hours    if col_hours    not in used else None
    col_notes    = col_notes    if col_notes    not in used else None

    # If col_task wasn't found after conflict resolution, try remaining headers
    if not col_task:
        remaining = [h for h in headers if h not in used]
        col_task = _find_col(remaining, "task", "description", "work", "notes", "detail", "activity")
        if col_task:
            used.add(col_task)

    if not col_date or not col_task:
        missing = []
        if not col_date: missing.append("Date")
        if not col_task: missing.append("Task/Description")
        raise HTTPException(
            status_code=422,
            detail=f"Cannot find required column(s): {', '.join(missing)}. Headers found: {headers}"
        )

    DATE_FMTS = ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"]

    def _parse_date(val) -> Optional[str]:
        if val is None or str(val).strip() in ("", "None"):
            return None
        # openpyxl / xlrd return Python datetime or date objects for date cells
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        # Excel serial numbers stored as float/int (rare, but happens with .xls edge cases)
        if isinstance(val, (int, float)):
            try:
                from datetime import datetime as _dt, timedelta as _td
                dt = _dt(1899, 12, 30) + _td(days=float(val))
                if 1970 <= dt.year <= 2100:  # sanity check
                    return dt.strftime("%Y-%m-%d")
            except (ValueError, OverflowError):
                pass
            return None
        s = str(val).strip()
        for fmt in DATE_FMTS:
            try:
                from datetime import datetime
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    imported = 0
    skipped = 0
    errors: list[str] = []

    for i, row in enumerate(rows, start=2):  # row 2 = first data row (1 = header)
        try:
            work_date = _parse_date(row.get(col_date, ""))
            task_desc = str(row.get(col_task, "")).strip()
            if not work_date or not task_desc:
                skipped += 1
                continue

            username = str(row.get(col_username, "") or user["username"]).strip() or user["username"]
            category = str(row.get(col_category, "") or "General").strip() or "General"
            notes    = str(row.get(col_notes, "") or "").strip()

            raw_qty = row.get(col_qty, "")
            try:
                quantity = int(float(str(raw_qty))) if raw_qty not in (None, "") else 1
            except (ValueError, TypeError):
                quantity = 1

            raw_hrs = row.get(col_hours, "")
            try:
                time_spent = float(str(raw_hrs)) if raw_hrs not in (None, "") else 0.0
            except (ValueError, TypeError):
                time_spent = 0.0

            add_production_log({
                "client_id":       client_id,
                "work_date":       work_date,
                "username":        username,
                "category":        category,
                "task_description": task_desc,
                "quantity":        quantity,
                "time_spent":      time_spent,
                "notes":           notes,
            })
            imported += 1
        except Exception as exc:
            errors.append(f"Row {i}: {str(exc)[:120]}")

    notify_activity(user["username"], "imported", "Time Tracking",
                    f"{imported} entries for client #{client_id}")
    return {"ok": True, "imported": imported, "skipped": skipped, "errors": errors}


@router.get("/production/report")
def production_report(client_id: Optional[int] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    report = get_production_report(scope, start_date, end_date)
    if user.get("role") == "admin" and client_id is not None and not (report.get("details") or []):
        report = get_production_report(None, start_date, end_date)
        report["fallback_all_clients"] = True
        report["selected_client_id"] = client_id
        return report
    report["fallback_all_clients"] = False
    report["selected_client_id"] = client_id
    return report


@router.get("/production/report/download")
def download_production_report(
    client_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hub_session: Optional[str] = Cookie(None),
):
    """Return a branded, printable HTML production report with MedPharma logo."""
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    data = get_production_report(scope, start_date, end_date)
    if user.get("role") == "admin" and client_id is not None and not (data.get("details") or []):
        data = get_production_report(None, start_date, end_date)

    from html import escape as _esc

    period_label = f"{start_date or 'All time'} — {end_date or 'today'}"
    by_user  = data.get("by_user", [])
    by_cat   = data.get("by_category", [])
    details  = data.get("details", [])
    flags    = data.get("time_management_flags", [])

    # ── Team summary rows ──────────────────────────────────────────────
    def _user_rows():
        if not by_user:
            return "<tr><td colspan='6' style='text-align:center;color:#9ca3af'>No team data for this period</td></tr>"
        return "".join(
            f"<tr><td><strong>{_esc(str(u.get('username','')))}</strong></td>"
            f"<td>{u.get('days_worked',0)}</td>"
            f"<td>{u.get('total_entries',0)}</td>"
            f"<td>{u.get('total_quantity',0)}</td>"
            f"<td>{u.get('total_hours',0)}h</td>"
            f"<td>{'⚠️ Low' if (u.get('avg_hours_per_day') or 0) < 6 else '✅ OK'} ({u.get('avg_hours_per_day',0)}h/day)</td></tr>"
            for u in by_user
        )

    def _cat_rows():
        if not by_cat:
            return "<tr><td colspan='4' style='text-align:center;color:#9ca3af'>No data</td></tr>"
        return "".join(
            f"<tr><td>{_esc(str(c.get('category','Uncategorized')))}</td>"
            f"<td>{c.get('total_entries',0)}</td>"
            f"<td>{c.get('total_quantity',0)}</td>"
            f"<td>{c.get('total_hours',0)}h</td></tr>"
            for c in by_cat
        )

    def _detail_rows():
        if not details:
            return "<tr><td colspan='7' style='text-align:center;color:#9ca3af'>No entries in this period</td></tr>"
        return "".join(
            f"<tr><td>{_esc(str(d.get('work_date','')))}</td>"
            f"<td>{_esc(str(d.get('username','')))}</td>"
            f"<td>{_esc(str(d.get('category','')))}</td>"
            f"<td>{_esc(str(d.get('task_description','')))}</td>"
            f"<td style='text-align:center'>{d.get('quantity',0)}</td>"
            f"<td style='text-align:center'>{d.get('time_spent',0)}h</td>"
            f"<td style='font-size:12px;color:#6b7280'>{_esc(str(d.get('notes','') or ''))}</td></tr>"
            for d in details
        )

    def _flag_section():
        if not flags:
            return ""
        rows = "".join(
            f"<tr><td>{_esc(str(f.get('username','')))}</td>"
            f"<td>{f.get('avg_hours_per_day',0)}h/day</td>"
            f"<td>{f.get('days_worked',0)}</td>"
            f"<td style='color:#dc2626'>{_esc(str(f.get('recommendation','')))}</td></tr>"
            for f in flags
        )
        return f"""
        <section class="section">
          <h2 style="color:#dc2626">⚠️ Time Management Alerts</h2>
          <table><thead><tr><th>Team Member</th><th>Avg Hrs/Day</th><th>Days Worked</th><th>Recommendation</th></tr></thead>
          <tbody>{rows}</tbody></table>
        </section>"""

    from datetime import date as _date
    generated = _date.today().strftime("%B %d, %Y")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>MedPharma Production Report — {_esc(period_label)}</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ font-family:'Segoe UI',Arial,sans-serif; font-size:13px; color:#1f2937; background:#fff; padding:0; }}
  .page {{ max-width:960px; margin:0 auto; padding:32px 24px; }}
  .header {{ display:flex; align-items:center; gap:20px; border-bottom:3px solid #1d4ed8; padding-bottom:20px; margin-bottom:28px; }}
  .logo-img {{ height:64px; width:auto; object-fit:contain; }}
  .header-text h1 {{ font-size:22px; font-weight:800; color:#1d4ed8; letter-spacing:-0.5px; }}
  .header-text p {{ font-size:13px; color:#6b7280; margin-top:4px; }}
  .meta-bar {{ display:flex; gap:28px; background:#f0f4ff; border-radius:8px; padding:12px 18px; margin-bottom:24px; font-size:13px; }}
  .meta-bar b {{ color:#1d4ed8; }}
  .section {{ margin-bottom:28px; }}
  .section h2 {{ font-size:15px; font-weight:700; color:#374151; margin-bottom:10px; padding-bottom:6px; border-bottom:1px solid #e5e7eb; }}
  table {{ width:100%; border-collapse:collapse; font-size:12.5px; }}
  th {{ background:#1d4ed8; color:#fff; text-align:left; padding:8px 10px; font-weight:600; font-size:12px; }}
  td {{ padding:7px 10px; border-bottom:1px solid #f3f4f6; }}
  tr:nth-child(even) td {{ background:#f9fafb; }}
  .footer {{ margin-top:36px; padding-top:14px; border-top:1px solid #e5e7eb; font-size:11px; color:#9ca3af; display:flex; justify-content:space-between; }}
  @media print {{
    body {{ padding:0; }}
    .page {{ padding:20px 16px; }}
    .no-print {{ display:none; }}
  }}
</style>
</head>
<body>
<div class="page">
  <div class="header">
    <img class="logo-img" src="https://medpharmasc.com/wp-content/uploads/2024/11/IMG_2392.png" alt="MedPharma Logo" crossorigin="anonymous">
    <div class="header-text">
      <h1>MedPharma Internal Hub</h1>
      <p>Team Production Report &nbsp;|&nbsp; {_esc(period_label)}</p>
    </div>
  </div>

  <div class="meta-bar">
    <span><b>Period:</b> {_esc(period_label)}</span>
    <span><b>Total Entries:</b> {len(details)}</span>
    <span><b>Team Members:</b> {len(by_user)}</span>
    <span><b>Generated:</b> {generated}</span>
  </div>

  {_flag_section()}

  <section class="section">
    <h2>👥 Team Summary</h2>
    <table>
      <thead><tr><th>Team Member</th><th>Days Worked</th><th>Total Entries</th><th>Items Completed</th><th>Total Hours</th><th>Pace</th></tr></thead>
      <tbody>{_user_rows()}</tbody>
    </table>
  </section>

  <section class="section">
    <h2>📊 Work by Category</h2>
    <table>
      <thead><tr><th>Category</th><th>Entries</th><th>Items</th><th>Hours</th></tr></thead>
      <tbody>{_cat_rows()}</tbody>
    </table>
  </section>

  <section class="section">
    <h2>📋 Detailed Daily Log</h2>
    <table>
      <thead><tr><th>Date</th><th>User</th><th>Category</th><th>Task Description</th><th>Qty</th><th>Hours</th><th>Notes</th></tr></thead>
      <tbody>{_detail_rows()}</tbody>
    </table>
  </section>

  <div class="footer">
    <span>MedPharma Internal Hub &nbsp;|&nbsp; <a href="https://medpharmasc.com">medpharmasc.com</a></span>
    <span class="no-print"><button onclick="window.print()" style="background:#1d4ed8;color:#fff;border:none;padding:6px 18px;border-radius:6px;cursor:pointer;font-size:13px">🖨️ Print / Save PDF</button></span>
    <span>Generated {generated}</span>
  </div>
</div>
</body>
</html>"""

    return Response(content=html, media_type="text/html")


@router.get("/notifications/status")
def notifications_status_endpoint(hub_session: Optional[str] = Cookie(None)):
    """Return the live notification channel configuration (admin only)."""
    _require_admin(hub_session)
    return get_notification_status()


@router.post("/notifications/test")
def notifications_test_endpoint(hub_session: Optional[str] = Cookie(None)):
    """Fire a real test notification through configured email/SMS channels.

    Admin only. Returns the per-channel delivery results plus current status.
    """
    user = _require_admin(hub_session)
    return send_test_notification(triggered_by=user.get("username") or "admin")


@router.post("/admin/production/relink-kindercare")
def relink_kindercare_production(body: ProductionRelinkIn, hub_session: Optional[str] = Cookie(None)):
    """Safely copy legacy production rows into the KinderCare account.

    This endpoint is idempotent: rows that already exist for KinderCare are skipped.
    """
    _require_admin(hub_session)

    usernames = [str(u or "").strip().lower() for u in (body.usernames or []) if str(u or "").strip()]
    if not usernames:
        usernames = ["mike", "sarah"]

    source_ids = [int(x) for x in (body.source_client_ids or []) if int(x) > 0]
    max_rows = max(1, min(int(body.max_rows or 5000), 20000))

    conn = get_db()
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        target = cur.execute(
            """
            SELECT id, username, company
            FROM clients
            WHERE lower(username)='kindercare' OR lower(company) LIKE '%kindercare%'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="KinderCare account not found")

        target_id = int(target["id"])
        target_existing = int(
            cur.execute("SELECT COUNT(*) FROM team_production WHERE client_id=?", (target_id,)).fetchone()[0]
        )

        conditions = ["client_id <> ?"]
        params: list[object] = [target_id]
        if source_ids:
            conditions.append("client_id IN ({})".format(",".join(["?"] * len(source_ids))))
            params.extend(source_ids)
        if usernames:
            conditions.append("lower(username) IN ({})".format(",".join(["?"] * len(usernames))))
            params.extend(usernames)

        where = " AND ".join(conditions)
        rows = cur.execute(
            f"""
            SELECT id, client_id, work_date, username, category, task_description, quantity, time_spent, notes
            FROM team_production
            WHERE {where}
            ORDER BY id ASC
            LIMIT ?
            """,
            [*params, max_rows],
        ).fetchall()

        copied = 0
        skipped_existing = 0
        sample = []
        for row in rows:
            exists = cur.execute(
                """
                SELECT 1
                FROM team_production
                WHERE client_id=?
                  AND work_date=?
                  AND lower(username)=lower(?)
                  AND category=?
                  AND task_description=?
                  AND IFNULL(quantity, 0)=IFNULL(?, 0)
                  AND ABS(IFNULL(time_spent, 0)-IFNULL(?, 0)) < 0.0001
                  AND IFNULL(notes, '')=IFNULL(?, '')
                LIMIT 1
                """,
                (
                    target_id,
                    row["work_date"],
                    row["username"],
                    row["category"],
                    row["task_description"],
                    row["quantity"],
                    row["time_spent"],
                    row["notes"],
                ),
            ).fetchone()
            if exists:
                skipped_existing += 1
                continue

            if not body.dry_run:
                cur.execute(
                    """
                    INSERT INTO team_production
                    (client_id, work_date, username, category, task_description, quantity, time_spent, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_id,
                        row["work_date"],
                        row["username"],
                        row["category"],
                        row["task_description"],
                        row["quantity"],
                        row["time_spent"],
                        row["notes"],
                    ),
                )
            copied += 1
            if len(sample) < 10:
                sample.append(
                    {
                        "source_row_id": int(row["id"]),
                        "source_client_id": int(row["client_id"]),
                        "work_date": row["work_date"],
                        "username": row["username"],
                        "category": row["category"],
                    }
                )

        if not body.dry_run:
            conn.commit()

        target_after = int(
            cur.execute("SELECT COUNT(*) FROM team_production WHERE client_id=?", (target_id,)).fetchone()[0]
        )
        return {
            "ok": True,
            "dry_run": bool(body.dry_run),
            "target": {"id": target_id, "username": target["username"], "company": target["company"]},
            "target_existing_before": target_existing,
            "source_candidates": len(rows),
            "copied": copied,
            "skipped_existing": skipped_existing,
            "target_after": target_after,
            "sample": sample,
        }
    finally:
        conn.close()


# ─── Files ────────────────────────────────────────────────────────────────────

@router.get("/files")
def get_files(client_id: Optional[int] = None, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    files = list_files(scope)
    return {"files": files}


@router.post("/files/upload")
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    category: str = Form("General"),
    description: str = Form(""),
    client_id: Optional[int] = Form(None),
    hub_session: Optional[str] = Cookie(None),
):
    user = _require_user(hub_session)
    scope = client_id if client_id is not None else (_client_scope(user) if _client_scope(user) is not None else user["id"])

    # Validate type
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".xlsx", ".xls", ".csv", ".pdf", ".doc", ".docx"):
        raise HTTPException(400, "Only .xlsx, .xls, .csv, .pdf, .doc, .docx files allowed")

    file_type = "excel" if ext in (".xlsx", ".xls", ".csv") else "pdf"
    unique_name = f"{uuid.uuid4().hex}{ext}"
    dest = os.path.join(UPLOAD_DIR, unique_name)

    content = await file.read()
    file_size = len(content)

    # Enforce upload size limit (50 MB)
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024
    if file_size > MAX_UPLOAD_SIZE:
        raise HTTPException(413, f"File too large. Maximum is {MAX_UPLOAD_SIZE // (1024*1024)}MB")

    with open(dest, "wb") as f:
        f.write(content)

    # Count rows for Excel/CSV
    row_count = 0
    if file_type == "excel":
        try:
            import csv, io
            if ext == ".csv":
                reader = csv.reader(io.StringIO(content.decode("utf-8", errors="replace")))
                row_count = max(0, sum(1 for _ in reader) - 1)
            else:
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
                    ws = wb.active
                    row_count = max(0, ws.max_row - 1)  # minus header
                    wb.close()
                except Exception:
                    row_count = 0
        except Exception:
            row_count = 0

    requested_category = (category or "General").strip() or "General"
    effective_category = requested_category
    category_source = "requested"
    infer_debug = None

    if file_type == "excel" and requested_category not in DATA_IMPORT_CATEGORIES:
        inferred, infer_debug = _infer_excel_category(content, ext, file.filename or "", description or "")
        if inferred in DATA_IMPORT_CATEGORIES:
            effective_category = inferred
            category_source = "auto"
        else:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "CATEGORY_INTERCEPT_REQUIRED",
                    "message": "Could not confidently map spreadsheet to Claims, Credentialing, Enrollment, or EDI. Select a valid category or upload a clearly labeled sheet.",
                    "requested_category": requested_category,
                    "category_inference": infer_debug,
                },
            )

    file_id = add_file(
        client_id=scope,
        filename=unique_name,
        original_name=file.filename or "file",
        file_type=file_type,
        file_size=file_size,
        category=effective_category,
        description=description,
        row_count=row_count,
        uploaded_by=user["username"],
    )

    # ── Auto-import data when category matches a known section and file is Excel/CSV ──
    imported = 0
    import_errors = []
    import_category = None
    if file_type == "excel" and effective_category in DATA_IMPORT_CATEGORIES:
        import_category = effective_category
        try:
            if effective_category == "Claims":
                imported, import_errors = _import_claims_from_excel(content, ext, scope)
            elif effective_category == "Credentialing":
                imported, import_errors = _import_credentialing_from_excel(content, ext, scope)
                if import_errors and any('header' in e.lower() or 'no rows' in e.lower() for e in import_errors):
                    import_errors.append("Required headers: Provider, Payor, Type, Status, Submitted, Follow Up, Approved, Expiration, Owner, Notes, Sub Profile")
            elif effective_category == "Enrollment":
                imported, import_errors = _import_enrollment_from_excel(content, ext, scope)
            elif effective_category == "EDI":
                imported, import_errors = _import_edi_from_excel(content, ext, scope)
        except Exception as e:
            import_errors = [str(e)]

    return {
        "id": file_id,
        "filename": unique_name,
        "original_name": file.filename,
        "requested_category": requested_category,
        "effective_category": effective_category,
        "category_source": category_source,
        "category_inference": infer_debug,
        "row_count": row_count,
        "imported": imported,
        "import_category": import_category,
        "import_errors": import_errors[:5],
    }


# ─── Credentialing Excel Template ─────────────────────────────────────────────
import io, csv
from fastapi.responses import StreamingResponse

@router.get("/files/template/credentialing")
def download_credentialing_template():
    headers = ["Provider", "Payor", "Type", "Status", "Submitted", "Follow Up", "Approved", "Expiration", "Owner", "Notes", "Sub Profile"]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerow(["John Smith", "Aetna", "Initial", "Submitted", "2026-02-20", "2026-02-27", "", "", "Jane Admin", "", "MHP"])
    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=credentialing_template.csv"}
    )


# ─── Client Report ────────────────────────────────────────────────────────────

def _build_section_data(conn, client_id, sub_profile=None, period=None):
    """Build claims/cred/enroll/edi/payments data for one filter set."""
    # Build parameterized filters
    sp_clause = ""
    sp_params = []
    if sub_profile:
        sp_clause = " AND sub_profile=?"
        sp_params = [sub_profile]

    date_clause = ""
    date_params = []
    if period == "mtd":
        from datetime import date as _d
        date_clause = " AND date(DOS) >= ?"
        date_params = [_d.today().replace(day=1).isoformat()]
    elif period == "ytd":
        from datetime import date as _d
        date_clause = " AND date(DOS) >= ?"
        date_params = [_d.today().replace(month=1, day=1).isoformat()]

    # Claims
    base = f"SELECT * FROM claims_master WHERE client_id=?{sp_clause}{date_clause}"
    claims = [dict(r) for r in conn.execute(base, [client_id] + sp_params + date_params).fetchall()]
    total_charged = sum(float(c.get("ChargeAmount") or 0) for c in claims)
    total_paid = sum(float(c.get("PaidAmount") or 0) for c in claims)
    total_balance = sum(float(c.get("BalanceRemaining") or 0) for c in claims)

    status_agg = {}
    for c in claims:
        st = c.get("ClaimStatus") or "Unknown"
        if st not in status_agg:
            status_agg[st] = {"count": 0, "charged": 0, "paid": 0}
        status_agg[st]["count"] += 1
        status_agg[st]["charged"] += float(c.get("ChargeAmount") or 0)
        status_agg[st]["paid"] += float(c.get("PaidAmount") or 0)
    by_status = [{"status": k, **v} for k, v in status_agg.items()]

    denial_agg = {}
    for c in claims:
        dc = c.get("DenialCategory") or ""
        if dc:
            denial_agg[dc] = denial_agg.get(dc, 0) + 1
    top_denials = sorted([{"category": k, "count": v} for k, v in denial_agg.items()], key=lambda x: -x["count"])[:10]

    # Credentialing
    cred_base = f"SELECT * FROM credentialing WHERE client_id=?{sp_clause}"
    cred_rows = [dict(r) for r in conn.execute(cred_base, [client_id] + sp_params).fetchall()]
    cred_summary = {}
    for r in cred_rows:
        st = r.get("Status") or "Unknown"
        cred_summary[st] = cred_summary.get(st, 0) + 1
    cred_detail = [{"provider": r.get("ProviderName",""), "payor": r.get("Payor",""), "type": r.get("CredType",""),
                    "status": r.get("Status",""), "submitted": r.get("SubmittedDate",""), "approved": r.get("ApprovedDate",""),
                    "expires": r.get("ExpirationDate",""), "owner": r.get("Owner","")} for r in cred_rows]

    # Enrollment
    enr_base = f"SELECT * FROM enrollment WHERE client_id=?{sp_clause}"
    enr_rows = [dict(r) for r in conn.execute(enr_base, [client_id] + sp_params).fetchall()]
    enr_summary = {}
    for r in enr_rows:
        st = r.get("Status") or "Unknown"
        enr_summary[st] = enr_summary.get(st, 0) + 1
    enr_detail = [{"provider": r.get("ProviderName",""), "payor": r.get("Payor",""), "type": r.get("EnrollType",""),
                   "status": r.get("Status",""), "submitted": r.get("SubmittedDate",""),
                   "effective": r.get("EffectiveDate",""), "owner": r.get("Owner","")} for r in enr_rows]

    # EDI
    edi_base = f"SELECT * FROM edi_setup WHERE client_id=?{sp_clause}"
    edi_rows = [dict(r) for r in conn.execute(edi_base, [client_id] + sp_params).fetchall()]
    edi_summary = {}
    for r in edi_rows:
        st = r.get("EDIStatus") or "Unknown"
        edi_summary[st] = edi_summary.get(st, 0) + 1
    edi_detail = [{"provider": r.get("ProviderName",""), "payor": r.get("Payor",""), "payer_id": r.get("PayerID",""),
                   "edi": r.get("EDIStatus",""), "era": r.get("ERAStatus",""), "eft": r.get("EFTStatus",""),
                   "submitted": r.get("SubmittedDate",""), "go_live": r.get("GoLiveDate",""),
                   "owner": r.get("Owner","")} for r in edi_rows]

    # Payments
    pay_rows = conn.execute("SELECT COALESCE(SUM(PaymentAmount),0) as total, COUNT(*) as cnt FROM payments WHERE client_id=?", (client_id,)).fetchone()
    payments = {"total": float(pay_rows["total"]) if pay_rows else 0, "count": int(pay_rows["cnt"]) if pay_rows else 0}

    return {
        "claims": {"total": len(claims), "total_charged": round(total_charged,2), "total_paid": round(total_paid,2),
                    "total_balance": round(total_balance,2), "by_status": by_status, "top_denials": top_denials},
        "credentialing": {"summary": [{"status":k,"count":v} for k,v in cred_summary.items()], "detail": cred_detail},
        "enrollment": {"summary": [{"status":k,"count":v} for k,v in enr_summary.items()], "detail": enr_detail},
        "edi": {"summary": [{"status":k,"count":v} for k,v in edi_summary.items()], "detail": edi_detail},
        "payments": payments,
    }


@router.get("/report/{client_id}")
def get_report(client_id: int, period: str = "all", sub_profile: Optional[str] = None,
               hub_session: Optional[str] = Cookie(None)):
    """Generate a comprehensive cross-section report for CSV / print, with sub-profile breakdowns."""
    _require_user(hub_session)
    from app.client_db import get_db
    from datetime import date, datetime

    conn = get_db()
    conn.row_factory = sqlite3.Row

    # Client info (including practice_type)
    client_row = conn.execute("SELECT company,contact_name,email,phone,practice_type FROM clients WHERE id=?", (client_id,)).fetchone()
    client_info = dict(client_row) if client_row else {}
    practice_type = client_info.get("practice_type", "") or ""

    try:
        # Build overall data — pass sub_profile and period as params (no f-string SQL)
        overall = _build_section_data(conn, client_id, sub_profile=sub_profile, period=period)

        # Build per-sub-profile breakdowns if MHP+OMT
        sub_profiles = {}
        if practice_type == "MHP+OMT" and not sub_profile:
            for sp_name in ["OMT", "MHP"]:
                sub_profiles[sp_name] = _build_section_data(conn, client_id, sub_profile=sp_name, period=period)
    finally:
        conn.close()

    result = {
        "generated_at": date.today().isoformat(),
        "period": period,
        "client": client_info,
        "practice_type": practice_type,
        **overall,
    }
    if sub_profiles:
        result["sub_profiles"] = sub_profiles

    return result


# ─── Direct Excel Import (per-section) ───────────────────────────────────────

@router.post("/import-excel")
async def import_excel(
    file: UploadFile = FastAPIFile(...),
    category: str = Form("Claims"),
    client_id: Optional[int] = Form(None),
    hub_session: Optional[str] = Cookie(None),
):
    """Import an Excel/CSV file directly into a data table (Claims, Credentialing, Enrollment, EDI).
    Also saves a copy of the file in Documents under the appropriate category."""
    user = _require_user(hub_session)
    scope = client_id if client_id is not None else (_client_scope(user) if _client_scope(user) is not None else user["id"])

    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".xlsx", ".xls", ".csv"):
        raise HTTPException(400, "Only .xlsx, .xls, .csv files supported for import")

    content = await file.read()

    # ── Save a copy of the file in Documents ──
    unique_name = f"{uuid.uuid4().hex}{ext}"
    dest = os.path.join(UPLOAD_DIR, unique_name)
    with open(dest, "wb") as f:
        f.write(content)
    file_size = len(content)
    row_count = 0
    try:
        import csv as _csv, io as _io
        if ext == ".csv":
            reader = _csv.reader(_io.StringIO(content.decode("utf-8", errors="replace")))
            row_count = max(0, sum(1 for _ in reader) - 1)
        else:
            import openpyxl
            wb = openpyxl.load_workbook(_io.BytesIO(content), read_only=True, data_only=True)
            row_count = max(0, sum(ws.max_row - 1 for ws in wb.worksheets if ws.max_row))
            wb.close()
    except Exception:
        pass
    file_id = add_file(
        client_id=scope, filename=unique_name, original_name=file.filename or "file",
        file_type="excel", file_size=file_size, category=category,
        description=f"{category} import — {file.filename}",
        row_count=row_count, uploaded_by=user["username"],
    )

    imported = 0
    errors = []

    try:
        if category == "Claims":
            imported, errors = _import_claims_from_excel(content, ext, scope)
        elif category == "Credentialing":
            imported, errors = _import_credentialing_from_excel(content, ext, scope)
        elif category == "Enrollment":
            imported, errors = _import_enrollment_from_excel(content, ext, scope)
        elif category == "EDI":
            imported, errors = _import_edi_from_excel(content, ext, scope)
        else:
            raise HTTPException(400, f"Unknown category: {category}")
    except HTTPException:
        raise
    except Exception as e:
        errors = [str(e)]

    # Notify admin of team imports
    if imported > 0:
        notify_bulk_activity(user["username"], "imported", category, imported,
                             f"File: {file.filename}")

    return {
        "category": category,
        "imported": imported,
        "errors": errors[:10],
        "original_name": file.filename,
        "file_id": file_id,
    }


def _parse_excel_rows(content: bytes, ext: str, combine_sheets: bool = True):
    """Parse Excel/CSV bytes into list of dict rows with smart header detection.
    If combine_sheets=True and multiple sheets share the same header structure,
    rows from all matching sheets are combined (useful for multi-tab claim files).
    Supports .xlsx (openpyxl), .xls (xlrd), and .csv."""
    import csv, io
    rows = []
    if ext == ".csv":
        reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")))
        rows = list(reader)
    elif ext == ".xls":
        # Legacy Excel (BIFF) — use xlrd
        import xlrd
        wb = xlrd.open_workbook(file_contents=content)
        sheet_results = []
        for sidx in range(wb.nsheets):
            ws = wb.sheet_by_index(sidx)
            if ws.nrows < 2:
                continue
            # Smart header detection: best row in first 10 rows
            header_row_idx = 0
            best_score = 0
            for ri in range(min(10, ws.nrows)):
                row_vals = [ws.cell_value(ri, ci) for ci in range(ws.ncols)]
                non_empty = sum(1 for v in row_vals if v is not None and str(v).strip())
                text = sum(1 for v in row_vals if isinstance(v, str) and str(v).strip())
                score = text * 2 + non_empty
                if score > best_score and non_empty >= 3:
                    best_score = score
                    header_row_idx = ri
            hdrs = [str(ws.cell_value(header_row_idx, ci)).strip() for ci in range(ws.ncols)]
            valid_cols = [i for i, h in enumerate(hdrs) if h]
            if len(valid_cols) < 2:
                continue
            sheet_rows = []
            for ri in range(header_row_idx + 1, ws.nrows):
                row_vals = []
                for ci in range(ws.ncols):
                    cell = ws.cell(ri, ci)
                    # Convert xlrd date serial to Python datetime
                    if cell.ctype == xlrd.XL_CELL_DATE:
                        try:
                            import datetime as _dt
                            dt = xlrd.xldate_as_datetime(cell.value, wb.datemode)
                            row_vals.append(dt)
                        except Exception:
                            row_vals.append(cell.value)
                    else:
                        row_vals.append(cell.value)
                if any(v is not None and str(v).strip() for v in row_vals):
                    sheet_rows.append(dict(zip(hdrs, row_vals)))
            if sheet_rows:
                hdr_key = tuple(sorted(h.lower() for h in hdrs if h))
                sheet_results.append((hdr_key, hdrs, sheet_rows))
        if sheet_results:
            if combine_sheets:
                from collections import defaultdict
                groups = defaultdict(list)
                for hdr_key, hdrs, srows in sheet_results:
                    groups[hdr_key].extend(srows)
                rows = max(groups.values(), key=len)
            else:
                rows = max(sheet_results, key=lambda x: len(x[2]))[2]
    else:
        # .xlsx — use openpyxl
        import openpyxl
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        except Exception as exc:
            raise ValueError(f"Cannot read Excel file: {exc}. If this is a .xls file, rename to .xls extension.") from exc
        # Collect parsed rows per sheet with their headers
        sheet_results = []  # list of (headers_tuple, rows_list)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            all_sheet_rows = []
            for row in ws.iter_rows(values_only=True):
                all_sheet_rows.append(row)
            if not all_sheet_rows:
                continue
            # Smart header detection: find the best header row
            header_row_idx = 0
            best_header_score = 0
            for idx, row in enumerate(all_sheet_rows[:10]):
                if not row:
                    continue
                non_empty = sum(1 for c in row if c is not None and str(c).strip())
                text_cells = sum(1 for c in row if c is not None and isinstance(c, str) and len(str(c).strip()) > 0)
                # Require at least 3 non-empty cells to be a real header row
                score = text_cells * 2 + non_empty
                if score > best_header_score and non_empty >= 3:
                    best_header_score = score
                    header_row_idx = idx
            if header_row_idx < len(all_sheet_rows):
                sheet_headers = [str(c).strip() if c else "" for c in all_sheet_rows[header_row_idx]]
                # Filter out empty header columns
                valid_cols = [i for i, h in enumerate(sheet_headers) if h]
                if len(valid_cols) < 2:
                    continue
                sheet_rows = []
                for row in all_sheet_rows[header_row_idx + 1:]:
                    if any(c is not None and str(c).strip() for c in row):
                        sheet_rows.append(dict(zip(sheet_headers, row)))
                if sheet_rows:
                    # Use a normalized header key (sorted, lowered) for grouping
                    hdr_key = tuple(sorted(h.lower() for h in sheet_headers if h))
                    sheet_results.append((hdr_key, sheet_headers, sheet_rows))

        if sheet_results:
            if combine_sheets:
                # Group sheets by header structure and combine sheets with same headers
                from collections import defaultdict
                groups = defaultdict(list)
                for hdr_key, hdrs, srows in sheet_results:
                    groups[hdr_key].extend(srows)
                # Pick the group with the most total rows
                best_group = max(groups.values(), key=len)
                rows = best_group
            else:
                # Pick single sheet with most rows
                best = max(sheet_results, key=lambda x: len(x[2]))
                rows = best[2]
        wb.close()
    return rows


def _norm_key(k):
    """Normalize an Excel header: lowercase, strip, collapse whitespace, remove _/-/# chars."""
    s = (k or "").strip().lower()
    s = s.replace("_", " ").replace("-", " ").replace("#", "").replace(".", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _fuzzy_match_column(header, col_map):
    """Try exact match first, then substring/contains matching as fallback."""
    norm = _norm_key(header)
    if norm in col_map:
        return col_map[norm]
    # Try if any map key is contained in the header
    for map_key, db_col in col_map.items():
        if len(map_key) >= 3 and map_key in norm:
            return db_col
    return None


def _clean_val(val):
    """Convert a cell value to a clean string. Strips time from datetime objects."""
    if val is None:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.isoformat()
    s = str(val).strip()
    # Strip trailing 00:00:00 from date strings like "2026-02-20 00:00:00"
    if len(s) > 10 and s[10:].strip() in ('00:00:00', '0:00:00', '00:00', '0:00'):
        s = s[:10]
    return s


def _import_credentialing_from_excel(content: bytes, ext: str, client_id: int):
    from app.client_db import get_db as _get_db
    from datetime import datetime as _dt_now

    # ── Status normalization — maps Excel values to dropdown filter values ──
    CRED_STATUS_NORMALIZE = {
        # Not Started
        "not started": "Not Started", "new": "Not Started", "none": "Not Started",
        "pending start": "Not Started", "not begun": "Not Started", "n/a": "Not Started",
        "to do": "Not Started", "open": "Not Started",
        # In Progress
        "in progress": "In Progress", "in-progress": "In Progress", "pending": "In Progress",
        "processing": "In Progress", "working": "In Progress", "active": "In Progress",
        "in process": "In Progress", "in review": "In Progress", "under review": "In Progress",
        "need action": "In Progress", "needs action": "In Progress", "action needed": "In Progress",
        "follow up": "In Progress", "follow-up": "In Progress", "followup": "In Progress",
        "waiting": "In Progress", "awaiting": "In Progress", "in queue": "In Progress",
        # Submitted
        "submitted": "Submitted", "sent": "Submitted", "filed": "Submitted",
        "application submitted": "Submitted", "app submitted": "Submitted",
        "mailed": "Submitted", "faxed": "Submitted", "uploaded": "Submitted",
        "received": "Submitted", "acknowledged": "Submitted",
        # Approved
        "approved": "Approved", "completed": "Approved", "credentialed": "Approved",
        "active - approved": "Approved", "accepted": "Approved", "enrolled": "Approved",
        "effective": "Approved", "live": "Approved", "done": "Approved",
        "granted": "Approved", "passed": "Approved",
        # Denied
        "denied": "Denied", "rejected": "Denied", "declined": "Denied",
        "not approved": "Denied", "failed": "Denied", "terminated": "Denied",
        "closed": "Denied", "cancelled": "Denied", "canceled": "Denied",
        # Expired
        "expired": "Expired", "lapsed": "Expired", "renewal needed": "Expired",
        "expiring": "Expired", "past due": "Expired", "overdue": "Expired",
        "renewal": "Expired", "recredentialing": "Expired",
    }

    VALID_CRED_STATUSES = {"Not Started", "In Progress", "Submitted", "Approved", "Denied", "Expired"}

    def _normalize_cred_status(raw):
        if not raw:
            return "Not Started"
        s = str(raw).strip()
        key = s.lower()
        if key in CRED_STATUS_NORMALIZE:
            return CRED_STATUS_NORMALIZE[key]
        for map_key, normalized in CRED_STATUS_NORMALIZE.items():
            if len(map_key) >= 4 and map_key in key:
                return normalized
        # If raw already matches a valid status (case-insensitive), use it
        for vs in VALID_CRED_STATUSES:
            if vs.lower() == key:
                return vs
        return "In Progress"  # Safe default for unrecognized statuses

    COL_MAP = {
        # Provider
        "provider": "ProviderName", "providername": "ProviderName", "provider name": "ProviderName",
        "rendering provider": "ProviderName", "doctor": "ProviderName", "physician": "ProviderName",
        "doctor name": "ProviderName", "physician name": "ProviderName", "practitioner": "ProviderName",
        "rendering": "ProviderName", "servicing provider": "ProviderName",
        "name": "ProviderName", "provider/doctor": "ProviderName",
        # Payor
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "insurance name": "Payor", "insurance company": "Payor", "plan": "Payor",
        "plan name": "Payor", "payer name": "Payor", "carrier": "Payor",
        "insurance plan": "Payor", "health plan": "Payor", "ins": "Payor",
        "primary insurance": "Payor", "primary payor": "Payor", "primary payer": "Payor",
        "ins name": "Payor", "ins company": "Payor",
        # Type / Subtask
        "type": "CredType", "credtype": "CredType", "cred type": "CredType",
        "credential type": "CredType", "credentialing type": "CredType",
        "application type": "CredType", "app type": "CredType",
        "subtask": "CredType", "sub task": "CredType", "task": "CredType",
        "cred subtask": "CredType", "task type": "CredType",
        # Status
        "status": "Status", "cred status": "Status", "credentialing status": "Status",
        "app status": "Status", "application status": "Status", "current status": "Status",
        # Dates
        "submitted": "SubmittedDate", "submitted date": "SubmittedDate", "submitteddate": "SubmittedDate",
        "date submitted": "SubmittedDate", "submission date": "SubmittedDate", "app submitted": "SubmittedDate",
        "application submitted": "SubmittedDate", "submit date": "SubmittedDate",
        "date": "SubmittedDate", "start date": "SubmittedDate",
        "date started": "SubmittedDate", "started": "SubmittedDate", "start": "SubmittedDate",
        "follow up": "FollowUpDate", "followupdate": "FollowUpDate", "follow up date": "FollowUpDate",
        "followup": "FollowUpDate", "followup date": "FollowUpDate", "next follow up": "FollowUpDate",
        "fu date": "FollowUpDate", "f/u date": "FollowUpDate", "f/u": "FollowUpDate",
        "next action date": "FollowUpDate", "due date": "FollowUpDate",
        "last follow up": "FollowUpDate", "last followup": "FollowUpDate",
        "last fu": "FollowUpDate", "last f/u": "FollowUpDate",
        "approved": "ApprovedDate", "approved date": "ApprovedDate", "approveddate": "ApprovedDate",
        "approval date": "ApprovedDate", "date approved": "ApprovedDate",
        "expiration": "ExpirationDate", "expires": "ExpirationDate", "expiration date": "ExpirationDate",
        "expirationdate": "ExpirationDate", "exp date": "ExpirationDate", "expiry": "ExpirationDate",
        "expiry date": "ExpirationDate", "renewal date": "ExpirationDate",
        "effective": "ApprovedDate", "effective date": "ApprovedDate",
        # Owner / Notes
        "owner": "Owner", "assigned to": "Owner", "assigned": "Owner", "coordinator": "Owner",
        "representative": "Owner", "rep": "Owner", "analyst": "Owner",
        "notes": "Notes", "comments": "Notes", "comment": "Notes", "remarks": "Notes",
        # Reference / Tracking
        "reference": "Notes", "tracking id": "Notes", "reference / tracking id": "Notes",
        "reference id": "Notes", "tracking": "Notes", "ref id": "Notes",
        # Sub-profile / LOB
        "sub profile": "sub_profile", "subprofile": "sub_profile", "sub_profile": "sub_profile",
        "lob": "sub_profile", "line of business": "sub_profile",
    }
    rows = _parse_excel_rows(content, ext)
    if not rows:
        return 0, ["No rows found"]
    first_row_keys = list(rows[0].keys()) if rows else []
    imported, errors = 0, []

    # Use a single connection for dedup + insert to avoid cross-connection visibility issues
    conn = _get_db()
    try:
        for i, row in enumerate(rows):
            mapped = {}
            for raw_key, val in row.items():
                db_col = _fuzzy_match_column(raw_key, COL_MAP)
                if db_col and val is not None:
                    mapped[db_col] = _clean_val(val)
            if not mapped.get("ProviderName") and not mapped.get("Payor"):
                continue
            # Normalize status to match filter dropdown values
            mapped["Status"] = _normalize_cred_status(mapped.get("Status", ""))
            try:
                existing = conn.execute(
                    "SELECT id FROM credentialing WHERE client_id=? AND ProviderName=? AND Payor=?",
                    (client_id, mapped.get("ProviderName", ""), mapped.get("Payor", ""))
                ).fetchone()
                if existing:
                    allowed = ["ProviderName","Payor","CredType","Status","SubmittedDate",
                               "FollowUpDate","ApprovedDate","ExpirationDate","Owner","Notes","sub_profile"]
                    parts, params = ["updated_at=?"], [_dt_now.now().isoformat()]
                    for f in allowed:
                        if f in mapped:
                            parts.append(f"{f}=?")
                            params.append(mapped[f])
                    params.append(existing["id"])
                    conn.execute(f"UPDATE credentialing SET {','.join(parts)} WHERE id=?", params)
                else:
                    conn.execute("""INSERT INTO credentialing
                        (client_id,ProviderName,Payor,CredType,Status,SubmittedDate,FollowUpDate,ApprovedDate,ExpirationDate,Owner,Notes,sub_profile)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (client_id, mapped.get("ProviderName",""), mapped.get("Payor",""),
                         mapped.get("CredType","Initial"), mapped.get("Status","Not Started"),
                         mapped.get("SubmittedDate",""), mapped.get("FollowUpDate",""),
                         mapped.get("ApprovedDate",""), mapped.get("ExpirationDate",""),
                         mapped.get("Owner",""), mapped.get("Notes",""), mapped.get("sub_profile","")))
                imported += 1
            except Exception as e:
                errors.append(f"Row {i+2}: {e}")
        conn.commit()
    finally:
        conn.close()

    if not imported and first_row_keys:
        errors.append(f"No rows matched. Excel headers found: {first_row_keys[:15]}")
        errors.append("Expected headers like: Provider, Payor/Insurance, Type, Status, Submitted, Follow Up, Approved, Expiration")
    return imported, errors


def _import_enrollment_from_excel(content: bytes, ext: str, client_id: int):
    from app.client_db import get_db as _get_db
    from datetime import datetime as _dt_now

    # ── Status normalization — maps Excel values to dropdown filter values ──
    ENROLL_STATUS_NORMALIZE = {
        # Not Started
        "not started": "Not Started", "new": "Not Started", "none": "Not Started",
        "pending start": "Not Started", "n/a": "Not Started", "to do": "Not Started",
        "open": "Not Started",
        # In Progress
        "in progress": "In Progress", "in-progress": "In Progress", "pending": "In Progress",
        "processing": "In Progress", "working": "In Progress", "active": "In Progress",
        "in process": "In Progress", "in review": "In Progress", "under review": "In Progress",
        "need action": "In Progress", "needs action": "In Progress", "action needed": "In Progress",
        "follow up": "In Progress", "follow-up": "In Progress", "followup": "In Progress",
        "waiting": "In Progress", "awaiting": "In Progress", "in queue": "In Progress",
        # Submitted
        "submitted": "Submitted", "sent": "Submitted", "filed": "Submitted",
        "application submitted": "Submitted", "app submitted": "Submitted",
        "mailed": "Submitted", "faxed": "Submitted", "uploaded": "Submitted",
        "received": "Submitted", "acknowledged": "Submitted",
        # Approved
        "approved": "Approved", "completed": "Approved", "credentialed": "Approved",
        "accepted": "Approved", "effective": "Approved", "live": "Approved",
        "done": "Approved", "granted": "Approved", "passed": "Approved",
        # Enrolled
        "enrolled": "Enrolled", "active - enrolled": "Enrolled", "participating": "Enrolled",
        "contracted": "Enrolled", "in network": "Enrolled", "in-network": "Enrolled",
        "par": "Enrolled",
        # Denied
        "denied": "Denied", "rejected": "Denied", "declined": "Denied",
        "not approved": "Denied", "failed": "Denied", "terminated": "Denied",
        "closed": "Denied", "cancelled": "Denied", "canceled": "Denied",
    }

    VALID_ENROLL_STATUSES = {"Not Started", "In Progress", "Submitted", "Approved", "Enrolled", "Denied"}

    def _normalize_enroll_status(raw):
        if not raw:
            return "Not Started"
        s = str(raw).strip()
        key = s.lower()
        if key in ENROLL_STATUS_NORMALIZE:
            return ENROLL_STATUS_NORMALIZE[key]
        for map_key, normalized in ENROLL_STATUS_NORMALIZE.items():
            if len(map_key) >= 4 and map_key in key:
                return normalized
        for vs in VALID_ENROLL_STATUSES:
            if vs.lower() == key:
                return vs
        return "In Progress"  # Safe default for unrecognized statuses

    COL_MAP = {
        # Provider
        "provider": "ProviderName", "providername": "ProviderName", "provider name": "ProviderName",
        "rendering provider": "ProviderName", "doctor": "ProviderName", "physician": "ProviderName",
        "doctor name": "ProviderName", "physician name": "ProviderName", "practitioner": "ProviderName",
        "rendering": "ProviderName", "servicing provider": "ProviderName",
        "name": "ProviderName",
        # Payor
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "insurance name": "Payor", "insurance company": "Payor", "plan": "Payor",
        "plan name": "Payor", "payer name": "Payor", "carrier": "Payor",
        "insurance plan": "Payor", "health plan": "Payor", "ins": "Payor",
        "primary insurance": "Payor", "primary payor": "Payor", "primary payer": "Payor",
        # Type
        "type": "EnrollType", "enrolltype": "EnrollType", "enroll type": "EnrollType",
        "enrollment type": "EnrollType", "application type": "EnrollType",
        # Status
        "status": "Status", "enrollment status": "Status", "enroll status": "Status",
        "app status": "Status", "application status": "Status", "current status": "Status",
        # Dates
        "submitted": "SubmittedDate", "submitted date": "SubmittedDate", "submitteddate": "SubmittedDate",
        "date submitted": "SubmittedDate", "submission date": "SubmittedDate", "submit date": "SubmittedDate",
        "application submitted": "SubmittedDate", "app submitted": "SubmittedDate",
        "follow up": "FollowUpDate", "followupdate": "FollowUpDate", "follow up date": "FollowUpDate",
        "followup": "FollowUpDate", "followup date": "FollowUpDate", "next follow up": "FollowUpDate",
        "fu date": "FollowUpDate", "f/u date": "FollowUpDate", "f/u": "FollowUpDate",
        "approved": "ApprovedDate", "approved date": "ApprovedDate", "approveddate": "ApprovedDate",
        "approval date": "ApprovedDate", "date approved": "ApprovedDate",
        "effective": "EffectiveDate", "effective date": "EffectiveDate", "effectivedate": "EffectiveDate",
        "eff date": "EffectiveDate", "start date": "EffectiveDate",
        # Owner / Notes
        "owner": "Owner", "assigned to": "Owner", "assigned": "Owner", "coordinator": "Owner",
        "notes": "Notes", "comments": "Notes", "comment": "Notes", "remarks": "Notes",
        "sub profile": "sub_profile", "subprofile": "sub_profile", "sub_profile": "sub_profile",
    }
    rows = _parse_excel_rows(content, ext)
    if not rows:
        return 0, ["No rows found"]
    first_row_keys = list(rows[0].keys()) if rows else []
    imported, errors = 0, []
    conn = _get_db()
    try:
        for i, row in enumerate(rows):
            mapped = {}
            for raw_key, val in row.items():
                db_col = _fuzzy_match_column(raw_key, COL_MAP)
                if db_col and val is not None:
                    mapped[db_col] = _clean_val(val)
            if not mapped.get("ProviderName") and not mapped.get("Payor"):
                continue
            # Normalize status to match filter dropdown values
            mapped["Status"] = _normalize_enroll_status(mapped.get("Status", ""))
            try:
                existing = conn.execute(
                    "SELECT id FROM enrollment WHERE client_id=? AND ProviderName=? AND Payor=?",
                    (client_id, mapped.get("ProviderName", ""), mapped.get("Payor", ""))
                ).fetchone()
                if existing:
                    allowed = ["ProviderName","Payor","EnrollType","Status","SubmittedDate",
                               "FollowUpDate","ApprovedDate","EffectiveDate","Owner","Notes","sub_profile"]
                    parts, params = ["updated_at=?"], [_dt_now.now().isoformat()]
                    for f in allowed:
                        if f in mapped:
                            parts.append(f"{f}=?")
                            params.append(mapped[f])
                    params.append(existing["id"])
                    conn.execute(f"UPDATE enrollment SET {','.join(parts)} WHERE id=?", params)
                else:
                    conn.execute("""INSERT INTO enrollment
                        (client_id,ProviderName,Payor,EnrollType,Status,SubmittedDate,FollowUpDate,ApprovedDate,EffectiveDate,Owner,Notes,sub_profile)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (client_id, mapped.get("ProviderName",""), mapped.get("Payor",""),
                         mapped.get("EnrollType","Enrollment"), mapped.get("Status","Not Started"),
                         mapped.get("SubmittedDate",""), mapped.get("FollowUpDate",""),
                         mapped.get("ApprovedDate",""), mapped.get("EffectiveDate",""),
                         mapped.get("Owner",""), mapped.get("Notes",""), mapped.get("sub_profile","")))
                imported += 1
            except Exception as e:
                errors.append(f"Row {i+2}: {e}")
        conn.commit()
    finally:
        conn.close()
    if not imported and first_row_keys:
        errors.append(f"No rows matched. Excel headers found: {first_row_keys[:15]}")
        errors.append("Expected headers like: Provider, Payor/Insurance, Type, Status, Submitted, Follow Up, Approved, Effective")
    return imported, errors


def _import_edi_from_excel(content: bytes, ext: str, client_id: int):
    from app.client_db import get_db as _get_db
    from datetime import datetime as _dt_now

    # ── Status normalization for EDI/ERA/EFT — maps to dropdown values ──
    EDI_STATUS_NORMALIZE = {
        # Not Started
        "not started": "Not Started", "new": "Not Started", "none": "Not Started",
        "pending start": "Not Started", "n/a": "Not Started", "to do": "Not Started",
        "open": "Not Started",
        # In Process
        "in process": "In Process", "in-process": "In Process", "in progress": "In Process",
        "in-progress": "In Process", "pending": "In Process", "processing": "In Process",
        "working": "In Process", "active": "In Process", "submitted": "In Process",
        "sent": "In Process", "filed": "In Process", "in review": "In Process",
        "under review": "In Process", "need action": "In Process", "needs action": "In Process",
        "waiting": "In Process", "awaiting": "In Process", "testing": "In Process",
        # Live
        "live": "Live", "completed": "Live", "active - live": "Live",
        "approved": "Live", "enrolled": "Live", "effective": "Live",
        "done": "Live", "connected": "Live", "enabled": "Live",
        "set up": "Live", "setup": "Live", "configured": "Live",
        "production": "Live", "go live": "Live", "go-live": "Live",
        # Failed
        "failed": "Failed", "denied": "Failed", "rejected": "Failed",
        "error": "Failed", "not accepted": "Failed", "terminated": "Failed",
        "closed": "Failed", "cancelled": "Failed", "canceled": "Failed",
        "expired": "Failed", "disconnected": "Failed",
    }

    VALID_EDI_STATUSES = {"Not Started", "In Process", "Live", "Failed"}

    def _normalize_edi_status(raw):
        if not raw:
            return "Not Started"
        s = str(raw).strip()
        key = s.lower()
        if key in EDI_STATUS_NORMALIZE:
            return EDI_STATUS_NORMALIZE[key]
        for map_key, normalized in EDI_STATUS_NORMALIZE.items():
            if len(map_key) >= 4 and map_key in key:
                return normalized
        for vs in VALID_EDI_STATUSES:
            if vs.lower() == key:
                return vs
        return "In Process"  # Safe default for unrecognized statuses

    COL_MAP = {
        # Provider
        "provider": "ProviderName", "providername": "ProviderName", "provider name": "ProviderName",
        "rendering provider": "ProviderName", "doctor": "ProviderName", "physician": "ProviderName",
        "doctor name": "ProviderName", "physician name": "ProviderName", "practitioner": "ProviderName",
        "name": "ProviderName",
        # Payor
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "insurance name": "Payor", "insurance company": "Payor", "plan": "Payor",
        "plan name": "Payor", "payer name": "Payor", "carrier": "Payor",
        "insurance plan": "Payor", "health plan": "Payor",
        # EDI-specific
        "payer id": "PayerID", "payerid": "PayerID", "payer_id": "PayerID",
        "edi": "EDIStatus", "edi status": "EDIStatus", "edistatus": "EDIStatus",
        "era": "ERAStatus", "era status": "ERAStatus", "erastatus": "ERAStatus",
        "eft": "EFTStatus", "eft status": "EFTStatus", "eftstatus": "EFTStatus",
        # Dates
        "submitted": "SubmittedDate", "submitted date": "SubmittedDate", "submitteddate": "SubmittedDate",
        "date submitted": "SubmittedDate", "submission date": "SubmittedDate",
        "go live": "GoLiveDate", "golivedate": "GoLiveDate", "go live date": "GoLiveDate",
        "go-live": "GoLiveDate", "golive": "GoLiveDate", "live date": "GoLiveDate",
        # Owner / Notes
        "owner": "Owner", "assigned to": "Owner", "assigned": "Owner", "coordinator": "Owner",
        "notes": "Notes", "comments": "Notes", "comment": "Notes", "remarks": "Notes",
        "sub profile": "sub_profile", "subprofile": "sub_profile", "sub_profile": "sub_profile",
    }
    rows = _parse_excel_rows(content, ext)
    if not rows:
        return 0, ["No rows found"]
    first_row_keys = list(rows[0].keys()) if rows else []
    imported, errors = 0, []
    conn = _get_db()
    try:
        for i, row in enumerate(rows):
            mapped = {}
            for raw_key, val in row.items():
                db_col = _fuzzy_match_column(raw_key, COL_MAP)
                if db_col and val is not None:
                    mapped[db_col] = _clean_val(val)
            if not mapped.get("ProviderName") and not mapped.get("Payor"):
                continue
            # Normalize all three EDI status fields to match dropdown values
            mapped["EDIStatus"] = _normalize_edi_status(mapped.get("EDIStatus", ""))
            mapped["ERAStatus"] = _normalize_edi_status(mapped.get("ERAStatus", ""))
            mapped["EFTStatus"] = _normalize_edi_status(mapped.get("EFTStatus", ""))
            try:
                existing = conn.execute(
                    "SELECT id FROM edi_setup WHERE client_id=? AND ProviderName=? AND Payor=?",
                    (client_id, mapped.get("ProviderName", ""), mapped.get("Payor", ""))
                ).fetchone()
                if existing:
                    allowed = ["ProviderName","Payor","EDIStatus","ERAStatus","EFTStatus",
                               "SubmittedDate","GoLiveDate","PayerID","Owner","Notes","sub_profile"]
                    parts, params = ["updated_at=?"], [_dt_now.now().isoformat()]
                    for f in allowed:
                        if f in mapped:
                            parts.append(f"{f}=?")
                            params.append(mapped[f])
                    params.append(existing["id"])
                    conn.execute(f"UPDATE edi_setup SET {','.join(parts)} WHERE id=?", params)
                else:
                    conn.execute("""INSERT INTO edi_setup
                        (client_id,ProviderName,Payor,EDIStatus,ERAStatus,EFTStatus,SubmittedDate,GoLiveDate,PayerID,Owner,Notes,sub_profile)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (client_id, mapped.get("ProviderName",""), mapped.get("Payor",""),
                         mapped.get("EDIStatus","Not Started"), mapped.get("ERAStatus","Not Started"),
                         mapped.get("EFTStatus","Not Started"),
                         mapped.get("SubmittedDate",""), mapped.get("GoLiveDate",""),
                         mapped.get("PayerID",""), mapped.get("Owner",""), mapped.get("Notes",""),
                         mapped.get("sub_profile","")))
                imported += 1
            except Exception as e:
                errors.append(f"Row {i+2}: {e}")
        conn.commit()
    finally:
        conn.close()
    if not imported and first_row_keys:
        errors.append(f"No rows matched. Excel headers found: {first_row_keys[:15]}")
        errors.append("Expected headers like: Provider, Payor/Insurance, Payer ID, EDI Status, ERA Status, EFT Status")
    return imported, errors


def _import_claims_from_excel(content: bytes, ext: str, client_id: int):
    """
    Parse an Excel/CSV claims report and upsert rows into claims_master.
    Flexible column matching — maps common header names to DB columns.
    Normalizes status values to match standard CLAIM_STATUSES.
    Returns (imported_count, error_list).
    """
    import csv, io
    from app.client_db import get_db
    from datetime import date as _date

    # Status normalization map — maps common Excel status values to standard statuses
    STATUS_NORMALIZE = {
        # Intake
        "intake": "Intake", "new": "Intake", "received": "Intake", "open": "Intake",
        "entered": "Intake", "created": "Intake", "registered": "Intake",
        # Verification
        "verification": "Verification", "verify": "Verification", "verifying": "Verification",
        "eligibility": "Verification", "elig check": "Verification", "auth": "Verification",
        "authorization": "Verification", "pre-auth": "Verification", "precert": "Verification",
        # Coding
        "coding": "Coding", "coded": "Coding", "code review": "Coding",
        "charge entry": "Coding", "charge review": "Coding",
        # Billed/Submitted
        "billed/submitted": "Billed/Submitted", "billed": "Billed/Submitted",
        "submitted": "Billed/Submitted", "filed": "Billed/Submitted", "sent": "Billed/Submitted",
        "pending": "Billed/Submitted", "in process": "Billed/Submitted",
        "in-process": "Billed/Submitted", "processing": "Billed/Submitted",
        "pending payment": "Billed/Submitted", "awaiting payment": "Billed/Submitted",
        "claim submitted": "Billed/Submitted", "billed to insurance": "Billed/Submitted",
        # Rejected
        "rejected": "Rejected", "reject": "Rejected", "returned": "Rejected",
        "kicked back": "Rejected", "not accepted": "Rejected", "error": "Rejected",
        "failed": "Rejected", "invalid": "Rejected",
        # Denied
        "denied": "Denied", "deny": "Denied", "denial": "Denied",
        "not covered": "Denied", "non-covered": "Denied",
        "denied - initial": "Denied", "initial denial": "Denied",
        # A/R Follow-Up
        "a/r follow-up": "A/R Follow-Up", "a/r follow up": "A/R Follow-Up",
        "ar follow up": "A/R Follow-Up", "ar follow-up": "A/R Follow-Up",
        "ar followup": "A/R Follow-Up", "follow up": "A/R Follow-Up",
        "follow-up": "A/R Follow-Up", "followup": "A/R Follow-Up",
        "in review": "A/R Follow-Up", "under review": "A/R Follow-Up",
        "pending review": "A/R Follow-Up", "working": "A/R Follow-Up",
        "in progress": "A/R Follow-Up",
        # Appeals
        "appeals": "Appeals", "appeal": "Appeals", "appealed": "Appeals",
        "appeal filed": "Appeals", "reconsideration": "Appeals",
        "corrected claim": "Appeals", "resubmitted": "Appeals",
        # Paid
        "paid": "Paid", "approved": "Paid", "finalized": "Paid",
        "payment received": "Paid", "closed - paid": "Paid",
        "settled": "Paid", "remitted": "Paid", "collected": "Paid",
        # Closed
        "closed": "Closed", "write off": "Closed", "write-off": "Closed",
        "written off": "Closed", "adjusted": "Closed", "void": "Closed",
        "voided": "Closed", "zero balance": "Closed", "closed - adjusted": "Closed",
    }

    def _normalize_status(raw):
        if not raw:
            return "Intake"
        s = str(raw).strip()
        key = s.lower()
        if key in STATUS_NORMALIZE:
            return STATUS_NORMALIZE[key]
        # Partial match: check if any known key is contained in the value
        for map_key, normalized in STATUS_NORMALIZE.items():
            if len(map_key) >= 4 and map_key in key:
                return normalized
        # If the raw value already matches a standard status (case-insensitive), use it
        from app.client_db import CLAIM_STATUSES
        for cs in CLAIM_STATUSES:
            if cs.lower() == key:
                return cs
        # Default: return as-is but log it
        return s

    COLUMN_MAP = {
        # ── ClaimKey ──
        "claimkey": "ClaimKey", "claim key": "ClaimKey", "claim": "ClaimKey",
        "claim id": "ClaimKey", "claimid": "ClaimKey", "claim number": "ClaimKey",
        "claim no": "ClaimKey", "claimno": "ClaimKey", "claim num": "ClaimKey",
        "account": "ClaimKey", "account number": "ClaimKey", "account no": "ClaimKey",
        "acct": "ClaimKey", "acct no": "ClaimKey", "acct number": "ClaimKey",
        "ticket": "ClaimKey", "ticket no": "ClaimKey", "ticket number": "ClaimKey",
        "reference": "ClaimKey", "ref": "ClaimKey", "ref no": "ClaimKey",
        "icn": "ClaimKey", "tcn": "ClaimKey", "dcn": "ClaimKey",
        # ── Patient ──
        "patientname": "PatientName", "patient name": "PatientName", "patient": "PatientName",
        "patientid": "PatientID", "patient id": "PatientID", "member id": "PatientID",
        "memberid": "PatientID", "member": "PatientName", "subscriber": "PatientName",
        "subscriber name": "PatientName", "insured name": "PatientName",
        "patient last name": "PatientName", "last name": "PatientName", "name": "PatientName",
        "first name": "PatientName",
        # ── Provider ──
        "providername": "ProviderName", "provider name": "ProviderName", "provider": "ProviderName",
        "rendering provider": "ProviderName", "rendering": "ProviderName",
        "servicing provider": "ProviderName", "doctor": "ProviderName", "physician": "ProviderName",
        "doctor name": "ProviderName", "physician name": "ProviderName", "practitioner": "ProviderName",
        "attending": "ProviderName", "attending provider": "ProviderName",
        "billing provider": "ProviderName", "referring provider": "ProviderName",
        "npi": "NPI", "provider npi": "NPI", "rendering npi": "NPI",
        # ── Payor / Insurance ──
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "insurance name": "Payor", "insurance company": "Payor", "ins": "Payor",
        "plan": "Payor", "plan name": "Payor", "payer name": "Payor",
        "carrier": "Payor", "insurance plan": "Payor", "health plan": "Payor",
        "primary insurance": "Payor", "primary payor": "Payor", "primary payer": "Payor",
        "primary payer name": "Payor", "primary insurance name": "Payor",
        "ins name": "Payor", "ins company": "Payor", "financial class": "Payor",
        "fc": "Payor", "fin class": "Payor", "payer type": "Payor",
        # ── DOS / CPT ──
        "dos": "DOS", "date of service": "DOS", "service date": "DOS",
        "svc date": "DOS", "from date": "DOS", "from": "DOS", "date from": "DOS",
        "service from": "DOS", "from dos": "DOS",
        "cptcode": "CPTCode", "cpt code": "CPTCode", "cpt": "CPTCode",
        "procedure": "CPTCode", "procedure code": "CPTCode", "proc code": "CPTCode",
        "proc": "CPTCode", "service code": "CPTCode", "hcpcs": "CPTCode",
        "description": "Description", "desc": "Description", "service description": "Description",
        "procedure description": "Description", "proc desc": "Description",
        "modifiers": "Description", "modifier": "Description", "mod": "Description",
        "scrub notes": "DenialReason", "scrub note": "DenialReason",
        "timely filing status": "ClaimStatus", "timely filing": "ClaimStatus",
        # ── Financials ──
        "chargeamount": "ChargeAmount", "charge amount": "ChargeAmount", "charge": "ChargeAmount",
        "billed": "ChargeAmount", "billed amount": "ChargeAmount", "total charge": "ChargeAmount",
        "total charges": "ChargeAmount", "charges": "ChargeAmount", "amount billed": "ChargeAmount",
        "gross charge": "ChargeAmount", "original amount": "ChargeAmount", "fee": "ChargeAmount",
        "allowedamount": "AllowedAmount", "allowed amount": "AllowedAmount", "allowed": "AllowedAmount",
        "approved amount": "AllowedAmount", "contracted amount": "AllowedAmount",
        "adjustmentamount": "AdjustmentAmount", "adjustment": "AdjustmentAmount", "adj": "AdjustmentAmount",
        "adjustment amount": "AdjustmentAmount", "adj amount": "AdjustmentAmount",
        "write off": "AdjustmentAmount", "writeoff": "AdjustmentAmount", "contractual": "AdjustmentAmount",
        "paidamount": "PaidAmount", "paid amount": "PaidAmount", "paid": "PaidAmount",
        "payment": "PaidAmount", "payment amount": "PaidAmount", "payments": "PaidAmount",
        "total paid": "PaidAmount", "total payments": "PaidAmount", "amount paid": "PaidAmount",
        "ins paid": "PaidAmount", "insurance paid": "PaidAmount", "reimbursement": "PaidAmount",
        "balanceremaining": "BalanceRemaining", "balance": "BalanceRemaining",
        "balance remaining": "BalanceRemaining", "bal": "BalanceRemaining",
        "ar balance": "BalanceRemaining", "outstanding": "BalanceRemaining",
        "amount due": "BalanceRemaining", "total balance": "BalanceRemaining",
        "patient balance": "BalanceRemaining", "ins balance": "BalanceRemaining",
        "remaining": "BalanceRemaining", "net balance": "BalanceRemaining",
        # ── Status / dates ──
        "claimstatus": "ClaimStatus", "claim status": "ClaimStatus", "status": "ClaimStatus",
        "current status": "ClaimStatus", "ar status": "ClaimStatus",
        "billdate": "BillDate", "bill date": "BillDate", "billed date": "BillDate",
        "date billed": "BillDate", "submission date": "BillDate", "submitted date": "BillDate",
        "date submitted": "BillDate",
        "denieddate": "DeniedDate", "denied date": "DeniedDate", "date denied": "DeniedDate",
        "denial date": "DeniedDate",
        "paiddate": "PaidDate", "paid date": "PaidDate", "date paid": "PaidDate",
        "payment date": "PaidDate", "check date": "PaidDate", "remit date": "PaidDate",
        "eob date": "PaidDate",
        "denialreason": "DenialReason", "denial reason": "DenialReason",
        "denial": "DenialReason", "reason": "DenialReason", "remark": "DenialReason",
        "remark code": "DenialReason", "carc": "DenialReason", "rarc": "DenialReason",
        "denial code": "DenialReason",
        "denialcategory": "DenialCategory", "denial category": "DenialCategory",
        "denial type": "DenialCategory",
        "owner": "Owner", "assigned to": "Owner", "assigned": "Owner", "worked by": "Owner",
        # ── Sub-profile ──
        "sub_profile": "sub_profile", "subprofile": "sub_profile", "sub profile": "sub_profile",
        "profile": "sub_profile", "practice profile": "sub_profile", "practice": "sub_profile",
    }

    def _parse_float(v):
        try:
            return float(str(v).replace("$", "").replace(",", "").strip())
        except Exception:
            return 0.0

    def _parse_date(v):
        if not v:
            return ""
        s = str(v).strip()
        # Handle datetime objects from openpyxl directly
        from datetime import datetime, date as _dt_date
        if isinstance(v, (datetime, _dt_date)):
            return v.strftime("%Y-%m-%d")
        # Strip time component if present (e.g. "2025-07-07 00:00:00")
        if " " in s and len(s) > 10:
            s = s.split(" ")[0]
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%Y%m%d",
                     "%d-%b-%Y", "%d-%b-%y", "%b %d, %Y", "%B %d, %Y",
                     "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return s

    # Parse rows using the shared smart parser (handles multi-sheet, smart header detection)
    rows = _parse_excel_rows(content, ext)

    if not rows:
        return 0, ["No rows found in file"]

    conn = get_db()
    cur = conn.cursor()
    today_str = _date.today().isoformat()
    imported = 0
    errors = []
    counter = 1

    try:
        for row in rows:
            # Use fuzzy column matching for flexible header support
            mapped = {}
            for raw_key, val in row.items():
                db_col = _fuzzy_match_column(raw_key, COLUMN_MAP)
                if db_col:
                    mapped[db_col] = val

            if not mapped:
                continue

            # Generate a ClaimKey if missing
            if not mapped.get("ClaimKey"):
                mapped["ClaimKey"] = f"IMP-{today_str}-{counter:04d}"
            counter += 1

            # Normalize claim status to standard values
            raw_status = mapped.get("ClaimStatus", "Intake")
            mapped["ClaimStatus"] = _normalize_status(raw_status)

            try:
                cur.execute("""
                    INSERT INTO claims_master
                    (client_id, ClaimKey, PatientID, PatientName, Payor, ProviderName, NPI,
                     DOS, CPTCode, Description, ChargeAmount, AllowedAmount, AdjustmentAmount,
                     PaidAmount, BalanceRemaining, ClaimStatus, BillDate, DeniedDate, PaidDate,
                     DenialCategory, DenialReason, Owner, StatusStartDate, LastTouchedDate, sub_profile)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(client_id, ClaimKey) DO UPDATE SET
                        PatientID=excluded.PatientID, PatientName=excluded.PatientName,
                        Payor=excluded.Payor, ProviderName=excluded.ProviderName, NPI=excluded.NPI,
                        DOS=excluded.DOS, CPTCode=excluded.CPTCode, Description=excluded.Description,
                        ChargeAmount=excluded.ChargeAmount, AllowedAmount=excluded.AllowedAmount,
                        AdjustmentAmount=excluded.AdjustmentAmount, PaidAmount=excluded.PaidAmount,
                        BalanceRemaining=excluded.BalanceRemaining, ClaimStatus=excluded.ClaimStatus,
                        BillDate=excluded.BillDate, DeniedDate=excluded.DeniedDate, PaidDate=excluded.PaidDate,
                        DenialCategory=excluded.DenialCategory, DenialReason=excluded.DenialReason,
                        Owner=excluded.Owner, LastTouchedDate=excluded.LastTouchedDate,
                        sub_profile=excluded.sub_profile, updated_at=CURRENT_TIMESTAMP
                """, (
                    client_id,
                    str(mapped.get("ClaimKey", "")),
                    str(mapped.get("PatientID", "")),
                    str(mapped.get("PatientName", "")),
                    str(mapped.get("Payor", "")),
                    str(mapped.get("ProviderName", "")),
                    str(mapped.get("NPI", "")),
                    _parse_date(mapped.get("DOS", "")),
                    str(mapped.get("CPTCode", "")),
                    str(mapped.get("Description", "")),
                    _parse_float(mapped.get("ChargeAmount", 0)),
                    _parse_float(mapped.get("AllowedAmount", 0)),
                    _parse_float(mapped.get("AdjustmentAmount", 0)),
                    _parse_float(mapped.get("PaidAmount", 0)),
                    _parse_float(mapped.get("BalanceRemaining", 0)),
                    str(mapped["ClaimStatus"]),
                    _parse_date(mapped.get("BillDate", "")),
                    _parse_date(mapped.get("DeniedDate", "")),
                    _parse_date(mapped.get("PaidDate", "")),
                    str(mapped.get("DenialCategory", "")),
                    str(mapped.get("DenialReason", "")),
                    str(mapped.get("Owner", "")),
                    today_str,
                    today_str,
                    str(mapped.get("sub_profile", "")),
                ))
                imported += 1
            except Exception as e:
                errors.append(f"Row {counter}: {e}")

        conn.commit()
    finally:
        conn.close()
    # Report unmapped headers as info for debugging
    if rows:
        first_row_keys = list(rows[0].keys())
        unmapped = [k for k in first_row_keys if not _fuzzy_match_column(k, COLUMN_MAP)]
        if unmapped:
            errors.append(f"Unmapped Excel columns (ignored): {unmapped[:10]}")
    return imported, errors


@router.get("/files/{file_id}/download")
def download_file(file_id: int, hub_session: Optional[str] = Cookie(None)):
    """Download the original uploaded file."""
    user = _require_user(hub_session)
    scope = _client_scope(user)
    rec = get_file_record(file_id, scope)
    if not rec:
        raise HTTPException(404, "File not found")
    path = os.path.join(UPLOAD_DIR, rec["filename"])
    if not os.path.isfile(path):
        raise HTTPException(404, "File not found on disk")
    from fastapi.responses import FileResponse
    return FileResponse(
        path,
        filename=rec["original_name"],
        media_type="application/octet-stream",
    )


@router.post("/files/{file_id}/replace")
async def replace_file(
    file_id: int,
    file: UploadFile = FastAPIFile(...),
    hub_session: Optional[str] = Cookie(None),
):
    """Replace an existing uploaded file with a new version.
    The old file is deleted from disk and replaced with the new upload.
    If it's an Excel in a data category, the data is re-imported."""
    user = _require_user(hub_session)
    scope = _client_scope(user)
    rec = get_file_record(file_id, scope)
    if not rec:
        raise HTTPException(404, "File not found")

    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".xlsx", ".xls", ".csv", ".pdf", ".doc", ".docx"):
        raise HTTPException(400, "Unsupported file type")

    content = await file.read()
    file_size = len(content)

    # Delete old file from disk
    old_path = os.path.join(UPLOAD_DIR, rec["filename"])
    if os.path.isfile(old_path):
        os.remove(old_path)

    # Save new file
    new_unique = f"{uuid.uuid4().hex}{ext}"
    new_path = os.path.join(UPLOAD_DIR, new_unique)
    with open(new_path, "wb") as f:
        f.write(content)

    # Count rows for Excel/CSV
    row_count = 0
    file_type = "excel" if ext in (".xlsx", ".xls", ".csv") else "pdf"
    if file_type == "excel":
        try:
            import csv as _csv, io as _io
            if ext == ".csv":
                reader = _csv.reader(_io.StringIO(content.decode("utf-8", errors="replace")))
                row_count = max(0, sum(1 for _ in reader) - 1)
            else:
                import openpyxl
                wb = openpyxl.load_workbook(_io.BytesIO(content), read_only=True, data_only=True)
                row_count = max(0, sum(ws.max_row - 1 for ws in wb.worksheets if ws.max_row))
                wb.close()
        except Exception:
            pass

    # Hard intercept (before replacing old file): never allow ambiguous excel routing.
    category = rec.get("category", "")
    effective_category = category
    category_source = "existing"
    infer_debug = None
    if file_type == "excel" and category not in DATA_IMPORT_CATEGORIES:
        inferred, infer_debug = _infer_excel_category(content, ext, file.filename or "", rec.get("description", "") or "")
        if inferred in DATA_IMPORT_CATEGORIES:
            effective_category = inferred
            category_source = "auto"
        else:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "CATEGORY_INTERCEPT_REQUIRED",
                    "message": "Could not confidently map replacement spreadsheet to Claims, Credentialing, Enrollment, or EDI. Set the file category first or upload a clearly labeled sheet.",
                    "existing_category": category,
                    "category_inference": infer_debug,
                },
            )

    # Update DB record
    update_file_record(file_id, {
        "filename": new_unique,
        "original_name": file.filename or rec["original_name"],
        "file_type": file_type,
        "file_size": file_size,
        "row_count": row_count,
        "uploaded_by": user["username"],
        "status": "Replaced",
        "category": effective_category,
    }, scope)

    # Auto re-import if data category
    imported = 0
    import_errors = []
    if file_type == "excel" and effective_category in DATA_IMPORT_CATEGORIES:
        try:
            if effective_category == "Claims":
                imported, import_errors = _import_claims_from_excel(content, ext, scope)
            elif effective_category == "Credentialing":
                imported, import_errors = _import_credentialing_from_excel(content, ext, scope)
            elif effective_category == "Enrollment":
                imported, import_errors = _import_enrollment_from_excel(content, ext, scope)
            elif effective_category == "EDI":
                imported, import_errors = _import_edi_from_excel(content, ext, scope)
        except Exception as e:
            import_errors = [str(e)]

    notify_activity(user["username"], "replaced file", "Documents",
                    f"{rec['original_name']} → {file.filename}")

    return {
        "ok": True,
        "file_id": file_id,
        "original_name": file.filename,
        "effective_category": effective_category,
        "category_source": category_source,
        "category_inference": infer_debug,
        "imported": imported,
        "import_errors": import_errors[:5],
    }


@router.delete("/files/{file_id}")
def delete_file(file_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    # Also delete the physical file from disk
    rec = get_file_record(file_id, scope)
    if rec:
        path = os.path.join(UPLOAD_DIR, rec["filename"])
        if os.path.isfile(path):
            os.remove(path)
    delete_file_record(file_id, scope)
    notify_activity(user["username"], "deleted file", "Documents",
                    rec["original_name"] if rec else "")
    return {"ok": True}


# ─── AI Report Generation (OpenAI GPT) ───────────────────────────────────────

@router.post("/report/{client_id}/ai-narrative")
async def generate_ai_narrative(client_id: int, hub_session: Optional[str] = Cookie(None)):
    """Send dashboard/report data to OpenAI GPT and return a professional narrative."""
    _require_user(hub_session)
    from app.config import OPENAI_API_KEY
    from app.client_db import get_db
    from datetime import date

    if not OPENAI_API_KEY:
        raise HTTPException(400, "OpenAI API key not configured. Set OPENAI_API_KEY environment variable.")

    # Gather all report data
    conn = get_db()
    conn.row_factory = sqlite3.Row
    client_row = conn.execute("SELECT company,contact_name,email,phone,practice_type,specialty FROM clients WHERE id=?", (client_id,)).fetchone()
    client_info = dict(client_row) if client_row else {}
    practice_type = client_info.get("practice_type", "") or ""

    try:
        overall = _build_section_data(conn, client_id)
        sub_profiles = {}
        if practice_type == "MHP+OMT":
            for sp_name in ["OMT", "MHP"]:
                sub_profiles[sp_name] = _build_section_data(conn, client_id, sub_profile=sp_name)
    finally:
        conn.close()

    # Build concise data summary for GPT
    cl = overall.get("claims", {})
    cred = overall.get("credentialing", {})
    enr = overall.get("enrollment", {})
    edi = overall.get("edi", {})
    pay = overall.get("payments", {})

    data_summary = f"""
PRACTICE: {client_info.get('company', 'Unknown')}
SPECIALTY: {client_info.get('specialty', 'N/A')}
PRACTICE TYPE: {practice_type or 'Standard'}
REPORT DATE: {date.today().isoformat()}

CLAIMS OVERVIEW:
- Total Claims: {cl.get('total', 0)}
- Total Charged: ${cl.get('total_charged', 0):,.2f}
- Total Paid: ${cl.get('total_paid', 0):,.2f}
- Outstanding A/R: ${cl.get('total_balance', 0):,.2f}
- Collection Rate: {round((cl.get('total_paid',0) / cl.get('total_charged',1)) * 100, 1) if cl.get('total_charged') else 0}%

CLAIMS BY STATUS:
{chr(10).join(f"  - {s.get('status','?')}: {s.get('count',0)} claims, ${s.get('charged',0):,.2f} charged, ${s.get('paid',0):,.2f} paid" for s in cl.get('by_status', []))}

TOP DENIAL CATEGORIES:
{chr(10).join(f"  - {d.get('category','?')}: {d.get('count',0)}" for d in cl.get('top_denials', [])) or '  None'}

CREDENTIALING: {len(cred.get('detail', []))} records
{chr(10).join(f"  - {c.get('provider','?')} / {c.get('payor','?')}: {c.get('status','?')}" for c in cred.get('detail', [])[:10])}

ENROLLMENT: {len(enr.get('detail', []))} records
{chr(10).join(f"  - {e.get('provider','?')} / {e.get('payor','?')}: {e.get('status','?')}" for e in enr.get('detail', [])[:10])}

EDI SETUP: {len(edi.get('detail', []))} connections
{chr(10).join(f"  - {e.get('provider','?')} / {e.get('payor','?')}: EDI={e.get('edi','?')}, ERA={e.get('era','?')}, EFT={e.get('eft','?')}" for e in edi.get('detail', [])[:10])}

PAYMENTS: {pay.get('count', 0)} payments totaling ${pay.get('total', 0):,.2f}
"""

    # Sub-profile data
    if sub_profiles:
        for sp_name, sp_data in sub_profiles.items():
            sc = sp_data.get("claims", {})
            data_summary += f"""
SUB-PROFILE: {sp_name}
  Claims: {sc.get('total', 0)} | Charged: ${sc.get('total_charged', 0):,.2f} | Paid: ${sc.get('total_paid', 0):,.2f} | AR: ${sc.get('total_balance', 0):,.2f}
  Credentialing: {len(sp_data.get('credentialing', {}).get('detail', []))} | Enrollment: {len(sp_data.get('enrollment', {}).get('detail', []))} | EDI: {len(sp_data.get('edi', {}).get('detail', []))}
"""

    system_prompt = """You are a senior Revenue Cycle Management (RCM) analyst at MedPharma SC, a healthcare credentialing and billing company. 
Write a detailed, professional narrative report that a healthcare practice owner can read and understand.

Your report should include:
1. EXECUTIVE SUMMARY — 2-3 sentences on overall account health
2. FINANCIAL PERFORMANCE — Analyze charges, collections, A/R, collection rate with context
3. CLAIMS ANALYSIS — Break down claim statuses, flag concerns, note denials
4. DENIAL MANAGEMENT — If denials exist, explain significance and recommend actions
5. CREDENTIALING STATUS — Summarize progress, flag pending items
6. ENROLLMENT STATUS — Summarize payor enrollment position
7. EDI CONNECTIVITY — Note setup status
8. SUB-PROFILE COMPARISON — If multiple sub-profiles exist, compare performance
9. RECOMMENDED ACTIONS — Specific, prioritized action items
10. OUTLOOK — Brief forward-looking statement

Write in a professional medical billing tone. Use specific numbers from the data.
Do NOT use markdown headers or bullets — write flowing paragraphs separated by blank lines, with key figures in bold (use <b> tags).
Keep it concise but thorough — aim for 400-600 words."""

    try:
        import openai
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Generate the narrative report based on this data:\n\n{data_summary}"}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        narrative = response.choices[0].message.content
        return {"narrative": narrative, "model": "gpt-4o-mini", "company": client_info.get("company", "")}
    except Exception as e:
        logging.getLogger(__name__).exception("AI narrative generation failed")
        raise HTTPException(500, "AI generation failed. Please try again later.")


# ─── PDF Report Generation ───────────────────────────────────────────────────

@router.api_route("/report/{client_id}/pdf", methods=["GET", "POST"])
async def download_report_pdf(client_id: int, period: str = "all", sub_profile: Optional[str] = None,
                              hub_session: Optional[str] = Cookie(None),
                              request: Request = None):
    """Generate and return a branded PDF report."""
    _require_user(hub_session)
    from app.client_db import get_db
    from datetime import date
    from io import BytesIO
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib.colors import HexColor
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
    from fastapi.responses import StreamingResponse

    # Extract narrative from POST body if available
    narrative = None
    if request and request.method == "POST":
        try:
            body = await request.json()
            narrative = body.get("narrative")
        except Exception:
            pass

    conn = get_db()
    conn.row_factory = sqlite3.Row

    client_row = conn.execute("SELECT company,contact_name,email,phone,practice_type,specialty FROM clients WHERE id=?", (client_id,)).fetchone()
    client_info = dict(client_row) if client_row else {}
    practice_type = client_info.get("practice_type", "") or ""
    company = client_info.get("company", "Client")

    try:
        overall = _build_section_data(conn, client_id, sub_profile=sub_profile, period=period)
        sub_profiles_data = {}
        if practice_type == "MHP+OMT" and not sub_profile:
            for sp_name in ["OMT", "MHP"]:
                sub_profiles_data[sp_name] = _build_section_data(conn, client_id, sub_profile=sp_name, period=period)
    finally:
        conn.close()

    cl = overall.get("claims", {})
    cred = overall.get("credentialing", {})
    enr = overall.get("enrollment", {})
    edi_data = overall.get("edi", {})
    pay = overall.get("payments", {})

    # Build PDF
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            topMargin=0.5*inch, bottomMargin=0.5*inch,
                            leftMargin=0.6*inch, rightMargin=0.6*inch)

    styles = getSampleStyleSheet()
    blue = HexColor("#0d47a1")
    light_blue = HexColor("#e3f2fd")
    dark = HexColor("#1a1a2e")
    gray = HexColor("#6b7280")
    green = HexColor("#059669")
    red = HexColor("#dc2626")
    white = HexColor("#ffffff")

    # Custom styles
    styles.add(ParagraphStyle('ReportTitle', parent=styles['Title'], fontSize=22, textColor=blue, spaceAfter=4, alignment=TA_LEFT))
    styles.add(ParagraphStyle('ReportSubtitle', parent=styles['Normal'], fontSize=10, textColor=gray, spaceAfter=16))
    styles.add(ParagraphStyle('SectionHead', parent=styles['Heading2'], fontSize=13, textColor=blue, spaceBefore=18, spaceAfter=8,
                               borderWidth=0, leftIndent=0))
    styles.add(ParagraphStyle('BodyText2', parent=styles['Normal'], fontSize=10, textColor=dark, leading=14, alignment=TA_JUSTIFY, spaceAfter=6))
    styles.add(ParagraphStyle('KPILabel', parent=styles['Normal'], fontSize=8, textColor=gray, alignment=TA_CENTER))
    styles.add(ParagraphStyle('KPIValue', parent=styles['Normal'], fontSize=16, textColor=blue, alignment=TA_CENTER, leading=20))
    styles.add(ParagraphStyle('SmallGray', parent=styles['Normal'], fontSize=8, textColor=gray))
    styles.add(ParagraphStyle('NarrativeText', parent=styles['Normal'], fontSize=10, textColor=dark, leading=15, alignment=TA_JUSTIFY, spaceAfter=8))

    story = []
    period_label = {"all": "All Time", "mtd": "Month to Date", "ytd": "Year to Date"}.get(period, period)

    # ── Header ──
    story.append(Paragraph(f"MedPharma SC", styles['ReportTitle']))
    story.append(Paragraph(f"Revenue Cycle Management & Credentialing Report", styles['ReportSubtitle']))
    story.append(HRFlowable(width="100%", thickness=2, color=blue, spaceAfter=12))
    story.append(Paragraph(f"<b>{company}</b> — {period_label} Report  |  Generated: {date.today().strftime('%B %d, %Y')}", styles['BodyText2']))
    if practice_type:
        story.append(Paragraph(f"Practice Type: {practice_type}  |  Specialty: {client_info.get('specialty', 'N/A')}", styles['SmallGray']))
    story.append(Spacer(1, 12))

    # ── KPI Summary Table ──
    coll_rate = round((cl.get('total_paid', 0) / cl.get('total_charged', 1)) * 100, 1) if cl.get('total_charged') else 0
    kpi_data = [
        [Paragraph('<b>Total Claims</b>', styles['KPILabel']),
         Paragraph('<b>Total Charged</b>', styles['KPILabel']),
         Paragraph('<b>Total Paid</b>', styles['KPILabel']),
         Paragraph('<b>Outstanding A/R</b>', styles['KPILabel']),
         Paragraph('<b>Collection Rate</b>', styles['KPILabel'])],
        [Paragraph(f"<font size='16' color='#0d47a1'><b>{cl.get('total', 0)}</b></font>", styles['KPIValue']),
         Paragraph(f"<font size='16' color='#7c3aed'><b>${cl.get('total_charged', 0):,.0f}</b></font>", styles['KPIValue']),
         Paragraph(f"<font size='16' color='#059669'><b>${cl.get('total_paid', 0):,.0f}</b></font>", styles['KPIValue']),
         Paragraph(f"<font size='16' color='#dc2626'><b>${cl.get('total_balance', 0):,.0f}</b></font>", styles['KPIValue']),
         Paragraph(f"<font size='16' color='#d97706'><b>{coll_rate}%</b></font>", styles['KPIValue'])]
    ]
    kpi_table = Table(kpi_data, colWidths=[doc.width/5]*5)
    kpi_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), light_blue),
        ('BOX', (0, 0), (-1, -1), 1, blue),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor("#bfdbfe")),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 16))

    # ── AI Narrative (if provided) ──
    if narrative:
        story.append(Paragraph("Overall Account Summary", styles['SectionHead']))
        # Clean HTML tags for reportlab compatibility
        clean = narrative.replace('\n\n', '<br/><br/>').replace('\n', '<br/>')
        clean = clean.replace('<b>', '<b>').replace('</b>', '</b>')
        story.append(Paragraph(clean, styles['NarrativeText']))
        story.append(Spacer(1, 8))

    # ── Claims by Status ──
    by_status = cl.get('by_status', [])
    if by_status:
        story.append(Paragraph("Claims by Status", styles['SectionHead']))
        tdata = [['Status', 'Count', 'Charged', 'Paid']]
        for s in by_status:
            tdata.append([s.get('status', ''), str(s.get('count', 0)),
                         f"${s.get('charged', 0):,.2f}", f"${s.get('paid', 0):,.2f}"])
        tdata.append(['TOTAL', str(cl.get('total', 0)),
                      f"${cl.get('total_charged', 0):,.2f}", f"${cl.get('total_paid', 0):,.2f}"])
        t = Table(tdata, colWidths=[doc.width*0.35, doc.width*0.15, doc.width*0.25, doc.width*0.25])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), blue),
            ('TEXTCOLOR', (0, 0), (-1, 0), white),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('BACKGROUND', (0, -1), (-1, -1), light_blue),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor("#e5e7eb")),
            ('BOX', (0, 0), (-1, -1), 1, blue),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))

    # ── Credentialing ──
    cred_detail = cred.get('detail', [])
    if cred_detail:
        story.append(Paragraph("Credentialing", styles['SectionHead']))
        tdata = [['Provider', 'Payor', 'Type', 'Status', 'Submitted', 'Approved']]
        for r in cred_detail[:20]:
            tdata.append([r.get('provider', '')[:25], r.get('payor', '')[:25], r.get('type', ''),
                         r.get('status', ''), r.get('submitted', '-'), r.get('approved', '-')])
        cw = [doc.width*0.2, doc.width*0.2, doc.width*0.12, doc.width*0.15, doc.width*0.16, doc.width*0.17]
        t = Table(tdata, colWidths=cw)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), blue), ('TEXTCOLOR', (0, 0), (-1, 0), white),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor("#e5e7eb")),
            ('BOX', (0, 0), (-1, -1), 1, blue),
            ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))

    # ── Enrollment ──
    enr_detail = enr.get('detail', [])
    if enr_detail:
        story.append(Paragraph("Enrollment", styles['SectionHead']))
        tdata = [['Provider', 'Payor', 'Type', 'Status', 'Submitted', 'Effective']]
        for r in enr_detail[:20]:
            tdata.append([r.get('provider', '')[:25], r.get('payor', '')[:25], r.get('type', ''),
                         r.get('status', ''), r.get('submitted', '-'), r.get('effective', '-')])
        enr_cw = [doc.width*0.2, doc.width*0.2, doc.width*0.12, doc.width*0.15, doc.width*0.16, doc.width*0.17]
        t = Table(tdata, colWidths=enr_cw)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), blue), ('TEXTCOLOR', (0, 0), (-1, 0), white),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor("#e5e7eb")),
            ('BOX', (0, 0), (-1, -1), 1, blue),
            ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))

    # ── EDI Setup ──
    edi_detail = edi_data.get('detail', [])
    if edi_detail:
        story.append(Paragraph("EDI Setup", styles['SectionHead']))
        tdata = [['Provider', 'Payor', 'Payer ID', 'EDI', 'ERA', 'EFT', 'Go-Live']]
        for r in edi_detail[:20]:
            tdata.append([r.get('provider', '')[:20], r.get('payor', '')[:20], r.get('payer_id', ''),
                         r.get('edi', ''), r.get('era', ''), r.get('eft', ''), r.get('go_live', '-')])
        edw = [doc.width*0.16]*7
        t = Table(tdata, colWidths=edw)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), blue), ('TEXTCOLOR', (0, 0), (-1, 0), white),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor("#e5e7eb")),
            ('BOX', (0, 0), (-1, -1), 1, blue),
            ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))

    # ── Sub-Profile Comparison ──
    if sub_profiles_data:
        story.append(Paragraph("Sub-Profile Comparison", styles['SectionHead']))
        tdata = [['Sub-Profile', 'Claims', 'Charged', 'Paid', 'A/R', 'Coll. Rate']]
        for sp_name, sp_d in sub_profiles_data.items():
            sc = sp_d.get('claims', {})
            sr = round((sc.get('total_paid', 0) / sc.get('total_charged', 1)) * 100, 1) if sc.get('total_charged') else 0
            tdata.append([sp_name, str(sc.get('total', 0)),
                         f"${sc.get('total_charged', 0):,.2f}", f"${sc.get('total_paid', 0):,.2f}",
                         f"${sc.get('total_balance', 0):,.2f}", f"{sr}%"])
        spw = [doc.width*0.18, doc.width*0.12, doc.width*0.2, doc.width*0.18, doc.width*0.18, doc.width*0.14]
        t = Table(tdata, colWidths=spw)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), blue), ('TEXTCOLOR', (0, 0), (-1, 0), white),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor("#e5e7eb")),
            ('BOX', (0, 0), (-1, -1), 1, blue),
            ('TOPPADDING', (0, 0), (-1, -1), 5), ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))

    # ── Payments ──
    story.append(Paragraph("Payments Summary", styles['SectionHead']))
    story.append(Paragraph(f"Total Payments: <b>${pay.get('total', 0):,.2f}</b>  |  Payment Count: <b>{pay.get('count', 0)}</b>", styles['BodyText2']))

    # ── Footer ──
    story.append(Spacer(1, 24))
    story.append(HRFlowable(width="100%", thickness=1, color=gray, spaceAfter=8))
    story.append(Paragraph(f"<i>This report was generated by MedPharma SC — Revenue Cycle Management & Credentialing Solutions</i>",
                           styles['SmallGray']))
    story.append(Paragraph(f"<i>Confidential — For internal use only  |  {date.today().strftime('%B %d, %Y')}</i>", styles['SmallGray']))

    doc.build(story)
    buf.seek(0)
    safe_name = company.replace(" ", "_").replace("/", "-")
    filename = f"{safe_name}_Report_{date.today().isoformat()}.pdf"
    return StreamingResponse(buf, media_type="application/pdf",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})


# ─── Bulk Claim Status Update ─────────────────────────────────────────────────

class BulkStatusIn(BaseModel):
    claim_ids: list[int]
    ClaimStatus: Optional[str] = None
    Owner: Optional[str] = None
    NextAction: Optional[str] = None
    NextActionDueDate: Optional[str] = None


@router.post("/claims/bulk-status")
def bulk_status_update(body: BulkStatusIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    data = {}
    if body.ClaimStatus:
        data["ClaimStatus"] = body.ClaimStatus
    if body.Owner:
        data["Owner"] = body.Owner
    if body.NextAction:
        data["NextAction"] = body.NextAction
    if body.NextActionDueDate:
        data["NextActionDueDate"] = body.NextActionDueDate
    updated = bulk_update_claims(body.claim_ids, data, scope)
    # Audit log
    log_audit(scope, user.get("username", ""), "bulk_status_update",
              "claims", None, f"Updated {updated} claims: {data}")
    notify_bulk_activity(user["username"], "bulk updated", "Claims", updated,
                         f"Status: {body.ClaimStatus or 'N/A'}, Owner: {body.Owner or 'N/A'}")
    return {"ok": True, "updated": updated}


# ─── Global Search ────────────────────────────────────────────────────────────

@router.get("/search")
def search(q: str = "", client_id: Optional[int] = None,
           hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    if not q or len(q) < 2:
        return {"results": []}
    results = global_search(q, scope)
    return {"results": results}


# ─── Alerts & Notifications ──────────────────────────────────────────────────

@router.get("/alerts")
def alerts_endpoint(client_id: Optional[int] = None,
                    hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    # Auto-flag SLA breaches before returning alerts
    auto_flag_sla(scope)
    alert_list = get_alerts(scope)
    return {"alerts": alert_list, "count": len(alert_list)}


# ─── Audit Log ────────────────────────────────────────────────────────────────

@router.get("/audit-log")
def audit_log_endpoint(client_id: Optional[int] = None, limit: int = 100,
                       hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    entries = get_audit_log(scope, limit)
    return {"entries": entries}


# ─── Export to CSV ────────────────────────────────────────────────────────────

@router.get("/export/{section}")
def export_section(section: str, client_id: Optional[int] = None,
                   sub_profile: Optional[str] = None,
                   hub_session: Optional[str] = Cookie(None)):
    """Export a section (claims, credentialing, enrollment, edi, providers, production) as CSV."""
    import csv, io
    from fastapi.responses import StreamingResponse

    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)

    if section == "claims":
        rows = export_claims(scope, sub_profile)
    elif section in ("credentialing", "enrollment", "edi_setup", "providers"):
        rows = export_table(section, scope)
    elif section == "production":
        logs = list_production_logs(scope)
        rows = logs
    else:
        raise HTTPException(400, f"Unknown section: {section}")

    if not rows:
        raise HTTPException(404, "No data to export")

    # Build CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)

    log_audit(scope, user.get("username", ""), "export",
              section, None, f"Exported {len(rows)} rows")

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={section}_export.csv"}
    )


# ─── Dashboard with Date Filters ─────────────────────────────────────────────

@router.get("/dashboard/filtered")
def dashboard_filtered(client_id: Optional[int] = None,
                       period: str = "all",
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       sub_profile: Optional[str] = None,
                       hub_session: Optional[str] = Cookie(None)):
    """Dashboard with date filtering support."""
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user)
    # Run SLA auto-flagging on dashboard load
    auto_flag_sla(scope)
    data = get_dashboard(scope, sub_profile=sub_profile,
                         date_from=start_date, date_to=end_date)
    data["user"] = user
    data["alerts"] = get_alerts(scope)
    return data