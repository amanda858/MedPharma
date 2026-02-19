"""Client Hub API — auth, claims queue, payments, notes, credentialing, enrollment, EDI, providers, dashboard."""

import os
import shutil
import uuid
from typing import Optional
from fastapi import APIRouter, HTTPException, Cookie, Response, Request, UploadFile, File as FastAPIFile, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from app.client_db import (
    authenticate, validate_session, logout_session,
    list_clients, create_client, update_client,
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
    if not hub_session:
        return None
    return validate_session(hub_session)


def _require_user(hub_session: Optional[str] = Cookie(None)):
    user = _get_user(hub_session)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def _require_admin(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user


def _client_scope(user: dict) -> Optional[int]:
    """Return client_id filter — None means all (admin)."""
    return None if user["role"] == "admin" else user["id"]


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
    if user["role"] == "admin":
        return list_clients()
    return [{"id": user["id"], "company": user["company"],
             "contact_name": user["contact_name"], "email": user["email"]}]


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


@router.get("/clients")
def get_clients(hub_session: Optional[str] = Cookie(None)):
    _require_admin(hub_session)
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


@router.get("/providers")
def get_providers(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    return list_providers(_client_scope(user))


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


@router.get("/claims")
def get_claims_list(status: Optional[str] = None, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    return get_claims(_client_scope(user), status)


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


@router.get("/credentialing")
def list_cred(status: Optional[str] = None, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    return get_credentialing(_client_scope(user), status)


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


@router.get("/enrollment")
def list_enroll(status: Optional[str] = None, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    return get_enrollment(_client_scope(user), status)


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


@router.get("/edi")
def list_edi(hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    return get_edi(_client_scope(user))


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
def dashboard_for_client(client_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    # Clients can only view their own dashboard
    if user["role"] != "admin" and user["id"] != client_id:
        raise HTTPException(403, "Forbidden")
    data = get_dashboard(client_id)
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
    if ext not in (".xlsx", ".xls", ".csv", ".pdf"):
        raise HTTPException(400, "Only .xlsx, .xls, .csv, .pdf files allowed")

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
    return {"id": file_id, "filename": unique_name, "original_name": file.filename, "row_count": row_count}


@router.delete("/files/{file_id}")
def delete_file(file_id: int, hub_session: Optional[str] = Cookie(None)):
    user = _require_user(hub_session)
    scope = _client_scope(user)
    delete_file_record(file_id, scope)
    return {"ok": True}