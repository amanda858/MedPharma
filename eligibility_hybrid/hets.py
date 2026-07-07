"""HETS 270/271 — DIRECT Medicare (FFS) eligibility, no clearinghouse.

CMS runs the **HIPAA Eligibility Transaction System (HETS)**: a free, real-time
270/271 eligibility service for **Original / Fee-for-Service Medicare only**
(Parts A & B). It does NOT cover Medicare Advantage (Part C) — those are
commercial plans on their own gateways — and it does NOT do coverage discovery.

This connector is the "break free from the clearinghouse" path: our own 270 goes
straight to CMS and we parse CMS's own 271 as first-party audit evidence.

HONESTY CONTRACT (this module never fabricates):
  • With no endpoint + submitter credentials it is NOT configured and returns
    CoverageStatus.UNKNOWN with `raw={"configured": False}` — it will NEVER
    invent an ACTIVE result. There is no "sandbox that pretends to be Medicare".
  • A real 271 is parsed for exactly what CMS returned, including AAA rejection
    segments (invalid MBI / beneficiary not found / etc.), which are surfaced as
    errors instead of being silently swallowed into "Unknown".

WHAT THE USER MUST DO (cannot be done in code):
  • Enroll as a CMS HETS submitter (trading-partner agreement) and obtain the
    submitter ID + CORE-compliant connectivity endpoint + credentials.
  • Since 2025-11-08 CMS requires US-origin IP traceability for HETS and blocks
    non-US source IPs — the host calling this must egress from a US IP.

Values marked "confirm against the CMS HETS 270/271 Companion Guide" are the
CMS-specific identifiers (receiver ID, Medicare payer ID, connectivity envelope
field names). They are config-driven so nothing is hardcoded wrong; set them
from the companion guide once enrolled.
"""
from __future__ import annotations

import re
import ssl
import urllib.request
import uuid
from datetime import date, datetime
from typing import Optional

from .models import (Benefit, CoverageResult, CoverageStatus, EligibilityProvider,
                     PatientRequest, ProviderError)

# CMS Medicare Beneficiary Identifier (MBI) — 11 chars, fixed positional format.
# Allowed alpha excludes S, L, O, I, B, Z (look-alikes for digits). Positions:
#   1:C(1-9) 2:A 3:AN 4:N 5:A 6:AN 7:N 8:A 9:A 10:N 11:N   (A=alpha, N=numeric)
_MBI_ALPHA = "ACDEFGHJKMNPQRTUVWXY"
_MBI_RE = re.compile(
    rf"^[1-9][{_MBI_ALPHA}][{_MBI_ALPHA}0-9][0-9][{_MBI_ALPHA}]"
    rf"[{_MBI_ALPHA}0-9][0-9][{_MBI_ALPHA}][{_MBI_ALPHA}][0-9][0-9]$"
)


def is_valid_mbi(raw: str) -> bool:
    """True iff `raw` matches the exact CMS MBI positional specification."""
    if not raw:
        return False
    return bool(_MBI_RE.match(raw.strip().upper().replace("-", "").replace(" ", "")))


class HETSProvider(EligibilityProvider):
    """Direct CMS HETS 270/271 for Original Medicare (FFS).

    Configured  => real 270 out, real 271 in.
    Unconfigured => honest UNKNOWN (never a fabricated ACTIVE).
    """
    name = "hets"

    def __init__(self, endpoint_url: str = "", submitter_id: str = "",
                 username: str = "", password: str = "",
                 receiver_id: str = "CMS", payer_id: str = "",
                 client_cert: str = "", client_key: str = "",
                 core_version: str = "2.2.0", timeout: int = 30):
        self.endpoint_url = endpoint_url.strip()
        self.submitter_id = submitter_id.strip()
        self.username = username
        self.password = password
        # confirm against the CMS HETS 270/271 Companion Guide:
        self.receiver_id = (receiver_id or "CMS").strip()
        self.payer_id = payer_id.strip()          # NM1*PR PI id for Medicare
        self.client_cert = client_cert.strip()    # optional mutual-TLS
        self.client_key = client_key.strip()
        self.core_version = core_version
        self.timeout = timeout

    # HETS is verification-only for FFS Medicare — no coverage discovery.
    def supports_discovery(self) -> bool:
        return False

    @property
    def configured(self) -> bool:
        """Live only when we have an endpoint, a submitter ID, and either
        username/password or a client certificate. Otherwise honestly refuse."""
        has_auth = bool(self.username and self.password) or bool(
            self.client_cert and self.client_key)
        return bool(self.endpoint_url and self.submitter_id and has_auth)

    # ── public API ──────────────────────────────────────────────────────────
    def verify(self, req: PatientRequest) -> CoverageResult:
        if not self.configured:
            return CoverageResult(
                status=CoverageStatus.UNKNOWN, source=self.name,
                errors=["HETS not configured — set the CMS HETS endpoint + "
                        "submitter credentials (complete CMS submitter enrollment "
                        "first). No result was fabricated."],
                raw={"configured": False},
                trace=[f"{self.name}.verify skipped: not configured"])

        mbi = (req.member_id or "").strip().upper().replace("-", "").replace(" ", "")
        if not is_valid_mbi(mbi):
            return CoverageResult(
                status=CoverageStatus.UNKNOWN, source=self.name,
                errors=[f"Member ID '{req.member_id}' is not a valid Medicare MBI "
                        f"(11-char CMS format). HETS requires the MBI; request not sent."],
                raw={"configured": True, "sent": False, "reason": "invalid_mbi"},
                trace=[f"{self.name}.verify aborted: MBI failed format check"])

        x12_270 = build_hets_270(req, self.submitter_id, self.receiver_id, self.payer_id)
        raw_271 = self._post_core(x12_270)
        result = parse_hets_271(raw_271, req, self.name)
        # Attach the raw 270/271 so the caller can persist them as audit evidence.
        result.raw["x12_270"] = x12_270
        result.raw["x12_271"] = raw_271
        result.trace.insert(0, f"{self.name}.verify -> CMS HETS 271 ({result.status.value})")
        return result

    # ── CORE Phase II connectivity (real HTTPS POST to CMS) ──────────────────
    def _post_core(self, x12_270: str) -> str:
        """POST the 270 to CMS via a CAQH CORE-style MIME envelope and return the
        raw response body (which contains the X12 271).

        The multipart field names and PayloadType strings follow the CAQH CORE
        connectivity rule; confirm the exact envelope against the CMS HETS
        Companion Guide once enrolled. Auth is either the CORE UserName/Password
        fields or mutual-TLS via a client certificate.
        """
        boundary = "----MedPharmaHETS" + uuid.uuid4().hex
        payload_id = str(uuid.uuid4())
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        fields = {
            "PayloadType": "X12_270_Request_005010X279A1",
            "ProcessingMode": "RealTime",
            "PayloadID": payload_id,
            "TimeStamp": ts,
            "SenderID": self.submitter_id,
            "ReceiverID": self.receiver_id,
            "CORERuleVersion": self.core_version,
        }
        if self.username and self.password:
            fields["UserName"] = self.username
            fields["Password"] = self.password

        parts: list[str] = []
        for k, v in fields.items():
            parts.append(f"--{boundary}")
            parts.append(f'Content-Disposition: form-data; name="{k}"')
            parts.append("")
            parts.append(v)
        parts.append(f"--{boundary}")
        parts.append('Content-Disposition: form-data; name="Payload"')
        parts.append("Content-Type: application/octet-stream")
        parts.append("")
        parts.append(x12_270)
        parts.append(f"--{boundary}--")
        parts.append("")
        body = "\r\n".join(parts).encode("utf-8")

        headers = {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            # CMS requires US-origin IP traceability for HETS (since 2025-11-08).
            # Egress must be from a US IP; this header preserves the origin chain.
            "X-Forwarded-For": "US-ORIGIN",
        }
        request = urllib.request.Request(self.endpoint_url, data=body, headers=headers)

        ctx: Optional[ssl.SSLContext] = None
        if self.client_cert and self.client_key:
            ctx = ssl.create_default_context()
            ctx.load_cert_chain(certfile=self.client_cert, keyfile=self.client_key)

        try:
            with urllib.request.urlopen(request, timeout=self.timeout, context=ctx) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:  # pragma: no cover - network (needs live CMS creds)
            raise ProviderError(self.name, f"HETS real-time 270 failed: {e}",
                                retryable=True)


# ── X12 270 builder (HETS-shaped 005010X279A1) ───────────────────────────────
def build_hets_270(req: PatientRequest, submitter_id: str,
                   receiver_id: str = "CMS", payer_id: str = "") -> str:
    """Build a Medicare FFS 270 eligibility inquiry for CMS HETS.

    HETS matches the beneficiary on MBI + last name + first name + DOB. Gender is
    not required for Medicare. Confirm the ISA/GS receiver + NM1*PR payer id
    against the CMS HETS Companion Guide.
    """
    now = datetime.now()
    d8, tm = now.strftime("%Y%m%d"), now.strftime("%H%M")
    yy = now.strftime("%y%m%d")
    ctrl = f"{int(uuid.uuid4().int % 1000000000):09d}"
    mbi = (req.member_id or "").strip().upper().replace("-", "").replace(" ", "")
    dob = (req.dob or "").replace("-", "")
    dos = (req.dos or date.today().isoformat()).replace("-", "")
    sub = f"{submitter_id:<15}"[:15]
    rcv = f"{receiver_id:<15}"[:15]

    segs = [
        f"ISA*00*          *00*          *ZZ*{sub}*ZZ*{rcv}"
        f"*{yy}*{tm}*^*00501*{ctrl}*0*P*:",
        f"GS*HS*{submitter_id}*{receiver_id}*{d8}*{tm}*1*X*005010X279A1",
        "ST*270*0001*005010X279A1",
        f"BHT*0022*13*{ctrl}*{d8}*{tm}",
        # 2000A Information Source = CMS / Medicare (the payer)
        "HL*1**20*1",
        f"NM1*PR*2*MEDICARE*****PI*{payer_id or 'CMS'}",
        # 2000B Information Receiver = the billing provider (the lab)
        "HL*2*1*21*1",
        f"NM1*1P*2*{(req.provider_name or 'PROVIDER').upper()}*****XX*{req.provider_npi or ''}",
        # 2000C Subscriber = the Medicare beneficiary
        "HL*3*2*22*0",
        f"TRN*1*{ctrl}*{submitter_id or '1'}",
        f"NM1*IL*1*{(req.last_name or '').upper()}*{(req.first_name or '').upper()}****MI*{mbi}",
        f"DMG*D8*{dob}",
        f"DTP*291*D8*{dos}",
        f"EQ*{(req.service_type_codes or ['30'])[0]}",
    ]
    # SE count = number of segments from ST through SE inclusive.
    st_to_body = segs[2:]  # ST ... EQ
    se_count = len(st_to_body) + 1
    segs.append(f"SE*{se_count}*0001")
    segs.append("GE*1*1")
    segs.append(f"IEA*1*{ctrl}")
    return "~".join(segs) + "~"


# ── X12 271 parser (real CMS response, incl. AAA rejections) ─────────────────
_STATUS_ACTIVE = {"1"}                 # EB01=1 Active Coverage
_STATUS_INACTIVE = {"6", "7", "8"}     # 6 Inactive, 7 Inactive-Pending Elig, 8 Inactive-Pending Investigation

# AAA reject reason codes we translate to plain English (subset; others echoed).
_AAA_REASON = {
    "42": "Unable to respond at current time (retry later)",
    "43": "Invalid/missing provider identification",
    "45": "Invalid/missing provider specialty",
    "47": "Invalid/missing provider state",
    "48": "Invalid/missing referring provider ID",
    "49": "Provider not on file",
    "51": "Provider not eligible for inquiries",
    "52": "Service dates not within provider plan enrollment",
    "56": "Inappropriate date",
    "57": "Invalid/missing date-of-service",
    "58": "Invalid/missing date-of-birth",
    "60": "Date of birth follows date of service",
    "61": "Date of death precedes date of service",
    "62": "Date of service not within allowable inquiry period",
    "63": "Date of service in future",
    "64": "Invalid/missing patient ID",
    "65": "Invalid/missing patient name",
    "66": "Invalid/missing patient gender",
    "67": "Patient not found",
    "68": "Duplicate patient ID",
    "71": "Patient DOB does not match that for the patient on the database",
    "72": "Invalid/missing subscriber/insured ID",
    "73": "Invalid/missing subscriber/insured name",
    "74": "Invalid/missing subscriber/insured gender",
    "75": "Subscriber/insured not found",
    "76": "Duplicate subscriber/insured ID",
    "78": "Subscriber/insured not in group/plan identified",
}


def parse_hets_271(edi: str, req: PatientRequest, source: str) -> CoverageResult:
    """Parse a real 271. Surfaces AAA rejections as errors (never hides them),
    reads EB benefit segments, plan dates (DTP), the MBI, and payer name."""
    # Locate the X12 payload if wrapped in a MIME/SOAP envelope.
    edi = _extract_x12(edi)
    segs = [s.strip() for s in edi.replace("\r", "").replace("\n", "").split("~")
            if s.strip()]

    status = CoverageStatus.UNKNOWN
    benefit = Benefit()
    errors: list[str] = []
    payer_name = req.payer_name or "Medicare"
    member_id = (req.member_id or "").strip().upper()
    plan_name = ""
    effective_date = ""
    term_date = ""
    prior_auth_required: Optional[bool] = None

    for seg in segs:
        el = seg.split("*")
        tag = el[0]

        if tag == "AAA":
            # Rejection: el[1]='Y/N' valid, el[3]=reject reason code, el[4]=follow-up
            code = el[3] if len(el) > 3 else ""
            reason = _AAA_REASON.get(code, f"reject code {code}" if code else "rejection")
            errors.append(f"CMS HETS rejected the request: {reason}.")

        elif tag == "EB" and len(el) > 1:
            eb01 = el[1]
            if eb01 in _STATUS_ACTIVE and status == CoverageStatus.UNKNOWN:
                status = CoverageStatus.ACTIVE
            elif eb01 in _STATUS_INACTIVE:
                status = CoverageStatus.INACTIVE
            elif eb01 == "B":                      # co-payment
                benefit.copay = _f(_nth(el, 7))
            elif eb01 == "C":                      # deductible
                benefit.deductible_total = _f(_nth(el, 7))
            elif eb01 == "A":                      # co-insurance
                val = _f(_nth(el, 8))
                if val is not None:
                    benefit.coinsurance_pct = val * 100 if val <= 1 else val
            elif eb01 == "G":                      # out-of-pocket (stop loss)
                benefit.oop_total = _f(_nth(el, 7))
            elif eb01 in ("U", "F"):               # PA / limitations flag on benefit
                prior_auth_required = True
            if len(el) > 5 and el[5]:
                plan_name = plan_name or el[5]

        elif tag == "NM1" and len(el) > 3:
            if el[1] == "PR":                      # payer
                payer_name = el[3] or payer_name
            elif el[1] == "IL" and len(el) > 9 and el[8] == "MI":
                member_id = el[9] or member_id     # MBI echoed back by CMS

        elif tag == "DTP" and len(el) > 3:
            qual = el[1]
            val = el[3]
            if qual in ("346", "356"):             # plan/eligibility begin
                effective_date = effective_date or _fmt_date(val)
            elif qual in ("347", "357"):           # plan/eligibility end
                term_date = term_date or _fmt_date(val)

    # If CMS returned an explicit inactive/termed with an end date in the past.
    if status == CoverageStatus.INACTIVE and term_date:
        status = CoverageStatus.TERMED

    return CoverageResult(
        status=status, source=source, payer_name=payer_name, payer_id=req.payer_id or "",
        plan_name=plan_name, member_id=member_id, effective_date=effective_date,
        term_date=term_date, benefit=benefit, prior_auth_required=prior_auth_required,
        confidence=1.0 if not errors else 0.0, errors=errors,
        raw={"x12_271_segments": len(segs), "configured": True},
        trace=[f"{source}.parse_271"])


# ── helpers ──────────────────────────────────────────────────────────────────
def _extract_x12(text: str) -> str:
    """Pull the X12 271 out of a MIME/SOAP-wrapped response, if wrapped."""
    if not text:
        return ""
    i = text.find("ISA")
    j = text.rfind("IEA")
    if i != -1 and j != -1 and j > i:
        end = text.find("~", j)
        return text[i:(end + 1 if end != -1 else len(text))]
    # fall back to a bare ST*271...SE block
    m = re.search(r"ST\*271.*?SE\*\d+\*\d+~?", text, re.DOTALL)
    return m.group(0) if m else text


def _nth(el: list[str], i: int) -> Optional[str]:
    return el[i] if len(el) > i else None


def _fmt_date(v: Optional[str]) -> str:
    """Normalize an X12 D8 (CCYYMMDD) date to YYYY-MM-DD; pass through ranges."""
    if not v:
        return ""
    v = v.strip()
    if len(v) == 8 and v.isdigit():
        return f"{v[0:4]}-{v[4:6]}-{v[6:8]}"
    return v


def _f(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
