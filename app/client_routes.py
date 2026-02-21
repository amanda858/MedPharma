"""Client Hub API — auth, claims queue, payments, notes, credentialing, enrollment, EDI, providers, dashboard."""

import os
import json as _json
import shutil
import sqlite3
import uuid
from typing import Optional
from fastapi import APIRouter, HTTPException, Cookie, Response, Request, UploadFile, File as FastAPIFile, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from app.client_db import (
    authenticate, validate_session, logout_session,
    list_clients, create_client, update_client,
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
    list_files, add_file, delete_file_record,
)

router = APIRouter(prefix="/hub/api")


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
    """Return client_id filter — None means all (any user sees all data)."""
    return None


# ─── Auth ─────────────────────────────────────────────────────────────────────

class LoginIn(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(body: LoginIn, response: Response):
    user, token = authenticate(body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    response.set_cookie("hub_session", token, httponly=True, samesite="lax", max_age=86400 * 30)
    return {"ok": True, "user": user}


@router.post("/logout")
def logout(response: Response, hub_session: Optional[str] = Cookie(None)):
    if hub_session:
        logout_session(hub_session)
    response.delete_cookie("hub_session")
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
    user = _require_user(hub_session)
    # Filter out admin accounts – only real client accounts should appear
    return [c for c in list_clients() if c.get("role") != "admin"]


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


# ─── Profile (own client profile) ──────────────────────────────────────────────────────────

@router.get("/profile")
def get_my_profile(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    cid = _client_scope(user) or user["id"]
    return get_profile(cid)


@router.get("/profile/{cid}")
def get_client_profile(cid: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    return get_profile(cid)


@router.put("/profile")
def update_my_profile(body: ProfileUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    cid = _client_scope(user) or user["id"]
    data = {k: v for k, v in body.model_dump().items() if v is not None and k != "doc_tabs"}
    if body.doc_tabs is not None:
        data["doc_tab_names"] = _json.dumps(body.doc_tabs)
    update_profile(cid, data)
    return {"ok": True}


@router.put("/profile/{cid}")
def update_client_profile(cid: int, body: ProfileUpdate, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    # Any authenticated user can edit any client profile
    data = {k: v for k, v in body.model_dump().items() if v is not None and k != "doc_tabs"}
    if body.doc_tabs is not None:
        data["doc_tab_names"] = _json.dumps(body.doc_tabs)
    update_profile(cid, data)
    return {"ok": True}


# ─── Practice Sub-Profiles ─────────────────────────────────────────────────────────────

@router.get("/practice-profiles")
def list_practice_profiles(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    cid = _client_scope(user) or user["id"]
    return {"profiles": get_practice_profiles(cid)}


@router.get("/practice-profiles/{cid}")
def list_practice_profiles_admin(cid: int, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    return {"profiles": get_practice_profiles(cid)}


@router.put("/practice-profiles/{profile_name}")
def save_practice_profile(profile_name: str, body: PracticeProfileUpdate,
                          hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    cid = _client_scope(user) or user["id"]
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
    cid = _client_scope(user) or user["id"]
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
    _require_admin(hub_session)
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
    return get_claims(scope, status, sub_profile=sub_profile)


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
    return {"id": cid, "ok": True}


@router.put("/claims/{claim_id}")
def edit_claim(claim_id: int, body: ClaimUpdate, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    update_claim(claim_id, {k: v for k, v in body.model_dump().items() if v is not None})
    return {"ok": True}


@router.delete("/claims/{claim_id}")
def remove_claim(claim_id: int, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    delete_claim(claim_id)
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
    cid = _client_scope(user) or user["id"]
    return get_payments(cid, claim_key)


@router.post("/claims/{claim_key}/payments")
def add_payment(claim_key: str, body: PaymentIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    cid = _client_scope(user) or user["id"]
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
    cid = _client_scope(user) or user["id"]
    return get_notes(cid, claim_key, module, ref_id)


@router.post("/notes")
def post_note(body: NoteIn, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    cid = _client_scope(user) or user["id"]
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
    return {"id": rid, "ok": True}


@router.put("/credentialing/{rid}")
def edit_cred(rid: int, body: CredUpdate, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    update_credentialing(rid, {k: v for k, v in body.model_dump().items() if v is not None})
    return {"ok": True}


@router.delete("/credentialing/{rid}")
def remove_cred(rid: int, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    delete_credentialing(rid)
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
    return {"id": eid, "ok": True}


@router.put("/enrollment/{rid}")
def edit_enroll(rid: int, body: EnrollUpdate, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    update_enrollment(rid, {k: v for k, v in body.model_dump().items() if v is not None})
    return {"ok": True}


@router.delete("/enrollment/{rid}")
def remove_enroll(rid: int, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    delete_enrollment(rid)
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
    return {"id": eid, "ok": True}


@router.put("/edi/{rid}")
def edit_edi(rid: int, body: EDIUpdate, hub_session: Optional[str] = Cookie(None)):
    _require_user(hub_session)
    update_edi(rid, {k: v for k, v in body.model_dump().items() if v is not None})
    return {"ok": True}


@router.delete("/edi/{rid}")
def remove_edi(rid: int, hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
    delete_edi(rid)
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
    scope = client_id or _client_scope(user) or user["id"]

    # Validate type
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".xlsx", ".xls", ".csv", ".pdf", ".doc", ".docx"):
        raise HTTPException(400, "Only .xlsx, .xls, .csv, .pdf, .doc, .docx files allowed")

    file_type = "excel" if ext in (".xlsx", ".xls", ".csv") else "pdf"
    unique_name = f"{uuid.uuid4().hex}{ext}"
    dest = os.path.join(UPLOAD_DIR, unique_name)

    content = await file.read()
    file_size = len(content)

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

    file_id = add_file(
        client_id=scope,
        filename=unique_name,
        original_name=file.filename or "file",
        file_type=file_type,
        file_size=file_size,
        category=category,
        description=description,
        row_count=row_count,
        uploaded_by=user["username"],
    )

    # ── Auto-import data when category matches a known section and file is Excel/CSV ──
    imported = 0
    import_errors = []
    import_category = None
    if file_type == "excel" and category in ("Claims", "Credentialing", "Enrollment", "EDI"):
        import_category = category
        try:
            if category == "Claims":
                imported, import_errors = _import_claims_from_excel(content, ext, scope)
            elif category == "Credentialing":
                imported, import_errors = _import_credentialing_from_excel(content, ext, scope)
            elif category == "Enrollment":
                imported, import_errors = _import_enrollment_from_excel(content, ext, scope)
            elif category == "EDI":
                imported, import_errors = _import_edi_from_excel(content, ext, scope)
        except Exception as e:
            import_errors = [str(e)]

    return {
        "id": file_id,
        "filename": unique_name,
        "original_name": file.filename,
        "row_count": row_count,
        "imported": imported,
        "import_category": import_category,
        "import_errors": import_errors[:5],
    }


# ─── Client Report ────────────────────────────────────────────────────────────

def _build_section_data(conn, client_id, sp_filter, where_date):
    """Build claims/cred/enroll/edi/payments data for one filter set."""
    # Claims
    base = f"SELECT * FROM claims_master WHERE client_id=?{sp_filter}{where_date}"
    claims = [dict(r) for r in conn.execute(base, (client_id,)).fetchall()]
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
    cred_base = f"SELECT * FROM credentialing WHERE client_id=?{sp_filter}"
    cred_rows = [dict(r) for r in conn.execute(cred_base, (client_id,)).fetchall()]
    cred_summary = {}
    for r in cred_rows:
        st = r.get("Status") or "Unknown"
        cred_summary[st] = cred_summary.get(st, 0) + 1
    cred_detail = [{"provider": r.get("ProviderName",""), "payor": r.get("Payor",""), "type": r.get("CredType",""),
                    "status": r.get("Status",""), "submitted": r.get("SubmittedDate",""), "approved": r.get("ApprovedDate",""),
                    "expires": r.get("ExpirationDate",""), "owner": r.get("Owner","")} for r in cred_rows]

    # Enrollment
    enr_base = f"SELECT * FROM enrollment WHERE client_id=?{sp_filter}"
    enr_rows = [dict(r) for r in conn.execute(enr_base, (client_id,)).fetchall()]
    enr_summary = {}
    for r in enr_rows:
        st = r.get("Status") or "Unknown"
        enr_summary[st] = enr_summary.get(st, 0) + 1
    enr_detail = [{"provider": r.get("ProviderName",""), "payor": r.get("Payor",""), "type": r.get("EnrollType",""),
                   "status": r.get("Status",""), "submitted": r.get("SubmittedDate",""),
                   "effective": r.get("EffectiveDate",""), "owner": r.get("Owner","")} for r in enr_rows]

    # EDI
    edi_base = f"SELECT * FROM edi_setup WHERE client_id=?{sp_filter}"
    edi_rows = [dict(r) for r in conn.execute(edi_base, (client_id,)).fetchall()]
    edi_summary = {}
    for r in edi_rows:
        st = r.get("EDIStatus") or "Unknown"
        edi_summary[st] = edi_summary.get(st, 0) + 1
    edi_detail = [{"provider": r.get("ProviderName",""), "payor": r.get("Payor",""), "payer_id": r.get("PayerID",""),
                   "edi": r.get("EDIStatus",""), "era": r.get("ERAStatus",""), "eft": r.get("EFTStatus",""),
                   "submitted": r.get("SubmittedDate",""), "go_live": r.get("GoLiveDate",""),
                   "owner": r.get("Owner","")} for r in edi_rows]

    # Payments
    pay_rows = conn.execute("SELECT COALESCE(SUM(PaidAmount),0) as total, COUNT(*) as cnt FROM payments WHERE client_id=?", (client_id,)).fetchone()
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

    # Period filter
    where_date = ""
    if period == "mtd":
        where_date = f" AND date(DOS) >= '{date.today().replace(day=1).isoformat()}'"
    elif period == "ytd":
        where_date = f" AND date(DOS) >= '{date.today().replace(month=1,day=1).isoformat()}'"

    sp_filter = f" AND sub_profile='{sub_profile}'" if sub_profile else ""

    # Client info (including practice_type)
    client_row = conn.execute("SELECT company,contact_name,email,phone,practice_type FROM clients WHERE id=?", (client_id,)).fetchone()
    client_info = dict(client_row) if client_row else {}
    practice_type = client_info.get("practice_type", "") or ""

    # Build overall data
    overall = _build_section_data(conn, client_id, sp_filter, where_date)

    # Build per-sub-profile breakdowns if MHP+OMT
    sub_profiles = {}
    if practice_type == "MHP+OMT" and not sub_profile:
        for sp_name in ["OMT", "MHP"]:
            sp_f = f" AND sub_profile='{sp_name}'"
            sub_profiles[sp_name] = _build_section_data(conn, client_id, sp_f, where_date)

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
    """Import an Excel/CSV file directly into a data table (Claims, Credentialing, Enrollment, EDI)."""
    user = _require_user(hub_session)
    scope = client_id or _client_scope(user) or user["id"]

    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".xlsx", ".xls", ".csv"):
        raise HTTPException(400, "Only .xlsx, .xls, .csv files supported for import")

    content = await file.read()
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

    return {
        "category": category,
        "imported": imported,
        "errors": errors[:10],
        "original_name": file.filename,
    }


def _parse_excel_rows(content: bytes, ext: str):
    """Parse Excel/CSV bytes into list of dict rows."""
    import csv, io
    rows = []
    if ext == ".csv":
        reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")))
        rows = list(reader)
    else:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        headers = None
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                headers = [str(c).strip() if c else "" for c in row]
            else:
                rows.append(dict(zip(headers, row)))
        wb.close()
    return rows


def _norm_key(k):
    return (k or "").strip().lower().replace("_", " ")


def _import_credentialing_from_excel(content: bytes, ext: str, client_id: int):
    from app.client_db import create_credentialing
    COL_MAP = {
        "provider": "ProviderName", "providername": "ProviderName", "provider name": "ProviderName",
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "type": "CredType", "credtype": "CredType", "cred type": "CredType",
        "credential type": "CredType", "credentialing type": "CredType",
        "status": "Status",
        "submitted": "SubmittedDate", "submitted date": "SubmittedDate", "submitteddate": "SubmittedDate",
        "follow up": "FollowUpDate", "followupdate": "FollowUpDate", "follow up date": "FollowUpDate",
        "approved": "ApprovedDate", "approved date": "ApprovedDate", "approveddate": "ApprovedDate",
        "expiration": "ExpirationDate", "expires": "ExpirationDate", "expiration date": "ExpirationDate",
        "expirationdate": "ExpirationDate",
        "owner": "Owner", "notes": "Notes",
        "sub profile": "sub_profile", "subprofile": "sub_profile", "sub_profile": "sub_profile",
    }
    rows = _parse_excel_rows(content, ext)
    if not rows:
        return 0, ["No rows found"]
    imported, errors = 0, []
    for i, row in enumerate(rows):
        mapped = {}
        for raw_key, val in row.items():
            db_col = COL_MAP.get(_norm_key(raw_key))
            if db_col and val is not None:
                mapped[db_col] = str(val).strip()
        if not mapped.get("ProviderName") and not mapped.get("Payor"):
            continue
        mapped["client_id"] = client_id
        try:
            create_credentialing(mapped)
            imported += 1
        except Exception as e:
            errors.append(f"Row {i+2}: {e}")
    return imported, errors


def _import_enrollment_from_excel(content: bytes, ext: str, client_id: int):
    from app.client_db import create_enrollment
    COL_MAP = {
        "provider": "ProviderName", "providername": "ProviderName", "provider name": "ProviderName",
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "type": "EnrollType", "enrolltype": "EnrollType", "enroll type": "EnrollType",
        "enrollment type": "EnrollType",
        "status": "Status",
        "submitted": "SubmittedDate", "submitted date": "SubmittedDate", "submitteddate": "SubmittedDate",
        "follow up": "FollowUpDate", "followupdate": "FollowUpDate", "follow up date": "FollowUpDate",
        "approved": "ApprovedDate", "approved date": "ApprovedDate", "approveddate": "ApprovedDate",
        "effective": "EffectiveDate", "effective date": "EffectiveDate", "effectivedate": "EffectiveDate",
        "owner": "Owner", "notes": "Notes",
        "sub profile": "sub_profile", "subprofile": "sub_profile", "sub_profile": "sub_profile",
    }
    rows = _parse_excel_rows(content, ext)
    if not rows:
        return 0, ["No rows found"]
    imported, errors = 0, []
    for i, row in enumerate(rows):
        mapped = {}
        for raw_key, val in row.items():
            db_col = COL_MAP.get(_norm_key(raw_key))
            if db_col and val is not None:
                mapped[db_col] = str(val).strip()
        if not mapped.get("ProviderName") and not mapped.get("Payor"):
            continue
        mapped["client_id"] = client_id
        try:
            create_enrollment(mapped)
            imported += 1
        except Exception as e:
            errors.append(f"Row {i+2}: {e}")
    return imported, errors


def _import_edi_from_excel(content: bytes, ext: str, client_id: int):
    from app.client_db import create_edi
    COL_MAP = {
        "provider": "ProviderName", "providername": "ProviderName", "provider name": "ProviderName",
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        "payer id": "PayerID", "payerid": "PayerID", "payer_id": "PayerID",
        "edi": "EDIStatus", "edi status": "EDIStatus", "edistatus": "EDIStatus",
        "era": "ERAStatus", "era status": "ERAStatus", "erastatus": "ERAStatus",
        "eft": "EFTStatus", "eft status": "EFTStatus", "eftstatus": "EFTStatus",
        "submitted": "SubmittedDate", "submitted date": "SubmittedDate", "submitteddate": "SubmittedDate",
        "go live": "GoLiveDate", "golivedate": "GoLiveDate", "go live date": "GoLiveDate",
        "go-live": "GoLiveDate",
        "owner": "Owner", "notes": "Notes",
        "sub profile": "sub_profile", "subprofile": "sub_profile", "sub_profile": "sub_profile",
    }
    rows = _parse_excel_rows(content, ext)
    if not rows:
        return 0, ["No rows found"]
    imported, errors = 0, []
    for i, row in enumerate(rows):
        mapped = {}
        for raw_key, val in row.items():
            db_col = COL_MAP.get(_norm_key(raw_key))
            if db_col and val is not None:
                mapped[db_col] = str(val).strip()
        if not mapped.get("ProviderName") and not mapped.get("Payor"):
            continue
        mapped["client_id"] = client_id
        try:
            create_edi(mapped)
            imported += 1
        except Exception as e:
            errors.append(f"Row {i+2}: {e}")
    return imported, errors


def _import_claims_from_excel(content: bytes, ext: str, client_id: int):
    """
    Parse an Excel/CSV claims report and upsert rows into claims_master.
    Flexible column matching — maps common header names to DB columns.
    Returns (imported_count, error_list).
    """
    import csv, io
    from app.client_db import get_db
    from datetime import date as _date

    COLUMN_MAP = {
        # ClaimKey
        "claimkey": "ClaimKey", "claim key": "ClaimKey", "claim #": "ClaimKey",
        "claim id": "ClaimKey", "claimid": "ClaimKey", "claim number": "ClaimKey",
        # Patient
        "patientname": "PatientName", "patient name": "PatientName", "patient": "PatientName",
        "patientid": "PatientID", "patient id": "PatientID",
        # Provider / Payor
        "providername": "ProviderName", "provider name": "ProviderName", "provider": "ProviderName",
        "npi": "NPI",
        "payor": "Payor", "payer": "Payor", "insurance": "Payor",
        # DOS / CPT
        "dos": "DOS", "date of service": "DOS", "service date": "DOS",
        "cptcode": "CPTCode", "cpt code": "CPTCode", "cpt": "CPTCode",
        "description": "Description", "desc": "Description",
        # Financials
        "chargeamount": "ChargeAmount", "charge amount": "ChargeAmount", "charge": "ChargeAmount", "billed": "ChargeAmount",
        "allowedamount": "AllowedAmount", "allowed amount": "AllowedAmount", "allowed": "AllowedAmount",
        "adjustmentamount": "AdjustmentAmount", "adjustment": "AdjustmentAmount", "adj": "AdjustmentAmount",
        "paidamount": "PaidAmount", "paid amount": "PaidAmount", "paid": "PaidAmount",
        "balanceremaining": "BalanceRemaining", "balance": "BalanceRemaining", "balance remaining": "BalanceRemaining",
        # Status / dates
        "claimstatus": "ClaimStatus", "claim status": "ClaimStatus", "status": "ClaimStatus",
        "billdate": "BillDate", "bill date": "BillDate",
        "denieddate": "DeniedDate", "denied date": "DeniedDate",
        "paiddate": "PaidDate", "paid date": "PaidDate",
        "denialreason": "DenialReason", "denial reason": "DenialReason", "denial": "DenialReason",
        "denialcategory": "DenialCategory", "denial category": "DenialCategory",
        "owner": "Owner",
        # Sub-profile (MHP or OMT for Luminary)
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
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%Y%m%d"):
            try:
                from datetime import datetime
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return s

    # Parse rows
    rows = []
    if ext == ".csv":
        reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")))
        rows = list(reader)
    else:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        headers = None
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                headers = [str(c).strip() if c else "" for c in row]
            else:
                rows.append(dict(zip(headers, row)))
        wb.close()

    if not rows:
        return 0, ["No rows found in file"]

    conn = get_db()
    cur = conn.cursor()
    today_str = _date.today().isoformat()
    imported = 0
    errors = []
    counter = 1

    for row in rows:
        # Normalize keys
        mapped = {}
        for raw_key, val in row.items():
            norm = (raw_key or "").strip().lower()
            db_col = COLUMN_MAP.get(norm)
            if db_col:
                mapped[db_col] = val

        if not mapped:
            continue

        # Generate a ClaimKey if missing
        if not mapped.get("ClaimKey"):
            mapped["ClaimKey"] = f"IMP-{today_str}-{counter:04d}"
        counter += 1

        try:
            cur.execute("""
                INSERT OR REPLACE INTO claims_master
                (client_id, ClaimKey, PatientID, PatientName, Payor, ProviderName, NPI,
                 DOS, CPTCode, Description, ChargeAmount, AllowedAmount, AdjustmentAmount,
                 PaidAmount, BalanceRemaining, ClaimStatus, BillDate, DeniedDate, PaidDate,
                 DenialCategory, DenialReason, Owner, StatusStartDate, LastTouchedDate, sub_profile)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                str(mapped.get("ClaimStatus", "Intake")),
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
    conn.close()
    return imported, errors


@router.delete("/files/{file_id}")
def delete_file(file_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    delete_file_record(file_id, scope)
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

    overall = _build_section_data(conn, client_id, "", "")
    sub_profiles = {}
    if practice_type == "MHP+OMT":
        for sp_name in ["OMT", "MHP"]:
            sp_filter = f" AND sub_profile='{sp_name}'"
            sub_profiles[sp_name] = _build_section_data(conn, client_id, sp_filter, "")
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
        raise HTTPException(500, f"AI generation failed: {str(e)}")


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

    # Period filter
    where_date = ""
    if period == "mtd":
        where_date = f" AND date(DOS) >= '{date.today().replace(day=1).isoformat()}'"
    elif period == "ytd":
        where_date = f" AND date(DOS) >= '{date.today().replace(month=1,day=1).isoformat()}'"
    sp_filter = f" AND sub_profile='{sub_profile}'" if sub_profile else ""

    client_row = conn.execute("SELECT company,contact_name,email,phone,practice_type,specialty FROM clients WHERE id=?", (client_id,)).fetchone()
    client_info = dict(client_row) if client_row else {}
    practice_type = client_info.get("practice_type", "") or ""
    company = client_info.get("company", "Client")

    overall = _build_section_data(conn, client_id, sp_filter, where_date)
    sub_profiles_data = {}
    if practice_type == "MHP+OMT" and not sub_profile:
        for sp_name in ["OMT", "MHP"]:
            sp_f = f" AND sub_profile='{sp_name}'"
            sub_profiles_data[sp_name] = _build_section_data(conn, client_id, sp_f, where_date)
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
        story.append(Paragraph("AI Account Summary", styles['SectionHead']))
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