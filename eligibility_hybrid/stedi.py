"""Stedi real-time eligibility (270/271 over JSON) — the fast, self-serve path.

HETS is the *direct* pipe to CMS, but it needs a multi-week CMS submitter
enrollment before it can send a single transaction. Stedi is the pragmatic
"out of the box" alternative: a modern REST eligibility API that is itself an
authorized clearinghouse, so you self-serve an API key in minutes and get
real-time Medicare **and** commercial 270/271 checks immediately.

  • JSON in, JSON out (Stedi translates to/from X12 for us) …
  • … and it *also* returns the raw X12 271, which we keep as audit evidence —
    the same first-party proof we store for the direct-HETS path.

HONESTY CONTRACT (this module never fabricates — identical to the HETS path):
  • With no API key + provider NPI it is NOT configured and returns
    CoverageStatus.UNKNOWN with `raw={"configured": False}`. It will NEVER
    invent an ACTIVE result. There is no "sandbox that pretends to be a payer".
  • For Medicare (payer id CMS) it requires a valid MBI and refuses to send an
    invalid one.
  • A real 271 is parsed for exactly what the payer returned, including payer
    AAA rejections, which are surfaced as errors instead of hidden.

WHAT THE USER MUST DO (cannot be done in code):
  • Create a Stedi account, generate an API key, and set STEDI_API_KEY +
    STEDI_PROVIDER_NPI (and STEDI_PROVIDER_NAME) in the server environment.
  • CMS still requires US-origin IP traceability for Medicare (HETS) checks, so
    the host calling Stedi must egress from a US IP; if the request is relayed
    through upstream systems, set STEDI_FORWARDED_FOR to the origin IP chain.

Endpoint + field names are from Stedi's published Real-Time Eligibility Check
(270/271 JSON) API; all identifiers are config-driven so nothing is hardcoded
wrong.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Optional

from .hets import _fmt_date, is_valid_mbi
from .models import (Benefit, CoverageResult, CoverageStatus, EligibilityProvider,
                     PatientRequest, ProviderError)

# Stedi benefit "code" (EB01) buckets — see Stedi active-coverage docs.
_ACTIVE_CODES = {"1", "2", "3", "4", "5"}      # 1 Active … 5 Active-Pending Invest.
_INACTIVE_CODES = {"6", "7", "8"}              # 6 Inactive, 7/8 Inactive-Pending
_FINANCIAL_CODES = {"B", "C", "A", "G"}        # copay / deductible / coins / OOP


class StediProvider(EligibilityProvider):
    """Stedi real-time 270/271 (JSON). Self-serve key, real payer responses.

    Configured  => real check out, real 271 (+ JSON) back.
    Unconfigured => honest UNKNOWN (never a fabricated ACTIVE).
    """
    name = "stedi"

    DEFAULT_ENDPOINT = (
        "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/eligibility/v3"
    )

    def __init__(self, api_key: str = "", endpoint_url: str = "",
                 payer_id: str = "CMS", provider_npi: str = "",
                 provider_name: str = "", forwarded_for: str = "",
                 timeout: int = 90):
        self.api_key = (api_key or "").strip()
        self.endpoint_url = (endpoint_url or self.DEFAULT_ENDPOINT).strip()
        self.payer_id = (payer_id or "CMS").strip()
        self.provider_npi = (provider_npi or "").strip()
        self.provider_name = (provider_name or "").strip()
        self.forwarded_for = (forwarded_for or "").strip()
        self.timeout = timeout

    # Stedi supports insurance discovery, but this connector is verify-only here.
    def supports_discovery(self) -> bool:
        return False

    @property
    def configured(self) -> bool:
        """Live only with an API key, an endpoint, and a provider NPI (Stedi
        requires the requesting provider's identifier). Otherwise honestly
        refuse."""
        return bool(self.api_key and self.endpoint_url and self.provider_npi)

    # ── public API ──────────────────────────────────────────────────────────
    def verify(self, req: PatientRequest) -> CoverageResult:
        if not self.configured:
            return CoverageResult(
                status=CoverageStatus.UNKNOWN, source=self.name,
                errors=["Stedi not configured — set STEDI_API_KEY and "
                        "STEDI_PROVIDER_NPI (generate a key in the Stedi portal). "
                        "No result was fabricated."],
                raw={"configured": False},
                trace=[f"{self.name}.verify skipped: not configured"])

        payer_id = (self.payer_id or req.payer_id or "CMS").strip()
        member = (req.member_id or "").strip()

        # Medicare (CMS) requires a valid MBI — refuse to send an invalid one.
        if payer_id.upper() == "CMS":
            mbi = member.upper().replace("-", "").replace(" ", "")
            if not is_valid_mbi(mbi):
                return CoverageResult(
                    status=CoverageStatus.UNKNOWN, source=self.name,
                    errors=[f"Member ID '{req.member_id}' is not a valid Medicare "
                            f"MBI (11-char CMS format). Request not sent to CMS."],
                    raw={"configured": True, "sent": False, "reason": "invalid_mbi"},
                    trace=[f"{self.name}.verify aborted: MBI failed format check"])
        elif not member:
            return CoverageResult(
                status=CoverageStatus.UNKNOWN, source=self.name,
                errors=["No member ID provided; request not sent."],
                raw={"configured": True, "sent": False, "reason": "no_member_id"},
                trace=[f"{self.name}.verify aborted: no member id"])

        body = build_stedi_request(
            req, payer_id,
            self.provider_npi or req.provider_npi,
            self.provider_name or req.provider_name)
        data = self._post(body)
        result = parse_stedi_response(data, req, self.name)
        # Keep the request + the payer's raw X12 271 as audit evidence.
        result.raw["request_json"] = json.dumps(body)
        result.trace.insert(0, f"{self.name}.verify -> Stedi 271 ({result.status.value})")
        return result

    # ── real HTTPS POST to Stedi ─────────────────────────────────────────────
    def _post(self, body: dict) -> dict:
        """POST the JSON eligibility request to Stedi and return parsed JSON.

        Raises ProviderError on network/HTTP failure so the caller records an
        error check instead of a fabricated result.
        """
        payload = json.dumps(body).encode("utf-8")
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # CMS traceability: only needed when there are upstream origin IPs.
        if self.forwarded_for:
            headers["X-Forwarded-For"] = self.forwarded_for
        request = urllib.request.Request(
            self.endpoint_url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as r:
                return json.loads(r.read().decode("utf-8", errors="replace") or "{}")
        except urllib.error.HTTPError as e:  # pragma: no cover - needs live key
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="replace")[:600]
            except Exception:
                pass
            raise ProviderError(self.name, f"Stedi HTTP {e.code}: {detail}",
                                retryable=e.code >= 500)
        except Exception as e:  # pragma: no cover - network
            raise ProviderError(self.name, f"Stedi request failed: {e}",
                                retryable=True)


# ── request builder (Stedi JSON) ─────────────────────────────────────────────
def build_stedi_request(req: PatientRequest, payer_id: str,
                        provider_npi: str, provider_name: str) -> dict:
    """Build the minimal Stedi eligibility request body from a PatientRequest.

    For Medicare (CMS) the member id is normalized to a bare MBI. Dates are sent
    in the compact CCYYMMDD form Stedi expects; date of service is omitted for
    "today" so behavior is consistent across payers.
    """
    dob = (req.dob or "").replace("-", "")
    member = (req.member_id or "").strip()
    if payer_id.upper() == "CMS":
        member = member.upper().replace("-", "").replace(" ", "")

    subscriber: dict = {}
    if member:
        subscriber["memberId"] = member
    if req.first_name:
        subscriber["firstName"] = req.first_name.strip()
    if req.last_name:
        subscriber["lastName"] = req.last_name.strip()
    if dob:
        subscriber["dateOfBirth"] = dob

    provider: dict = {"npi": (provider_npi or "").strip()}
    if provider_name:
        provider["organizationName"] = provider_name.strip()

    body: dict = {
        "tradingPartnerServiceId": payer_id,
        "provider": provider,
        "subscriber": subscriber,
        "encounter": {"serviceTypeCodes": req.service_type_codes or ["30"]},
    }
    dos = (req.date_of_service or "").replace("-", "")
    if dos:
        body["encounter"]["dateOfService"] = dos
    return body


# ── response parser (Stedi JSON 271) ─────────────────────────────────────────
def parse_stedi_response(data: dict, req: PatientRequest, source: str) -> CoverageResult:
    """Parse Stedi's JSON 271. Surfaces payer AAA rejections as errors (never
    hides them), reads benefit amounts, plan dates, the echoed member id, and
    payer name. Keeps the raw X12 271 for audit."""
    data = data or {}
    bi = data.get("benefitsInformation") or []

    errors: list[str] = []
    for e in (data.get("errors") or []):
        if not isinstance(e, dict):
            continue
        msg = e.get("description") or e.get("code") or ""
        loc = e.get("location") or ""
        if msg:
            errors.append("Payer rejected the request: "
                          f"{msg}" + (f" ({loc})" if loc else "") + ".")
    if str(data.get("status") or "").upper() == "ERROR":
        errors.append("Stedi could not deliver the request to the payer "
                      "(validation/transport error).")

    codes = [str(x.get("code") or "") for x in bi if isinstance(x, dict)]
    has_active = any(c in _ACTIVE_CODES for c in codes)
    has_inactive = any(c in _INACTIVE_CODES for c in codes)
    has_financial = any(c in _FINANCIAL_CODES for c in codes)

    if has_active:
        status = CoverageStatus.ACTIVE
    elif has_inactive:
        status = CoverageStatus.INACTIVE
    elif has_financial:
        status = CoverageStatus.ACTIVE          # financial benefits imply coverage
    else:
        status = CoverageStatus.UNKNOWN

    benefit = Benefit()
    benefit.copay = _pick_amount(bi, "B", "benefitAmount")
    benefit.deductible_total = _pick_amount(bi, "C", "benefitAmount")
    benefit.oop_total = _pick_amount(bi, "G", "benefitAmount")
    coins = _pick_amount(bi, "A", "benefitPercent")
    if coins is not None:
        benefit.coinsurance_pct = coins * 100 if coins <= 1 else coins

    prior_auth_required: Optional[bool] = None
    if any((x.get("authOrCertIndicator") or "").upper() == "Y"
           for x in bi if isinstance(x, dict)):
        prior_auth_required = True

    pdi = data.get("planDateInformation") or {}
    effective_date = _pick_date(
        pdi, ["eligibilityBegin", "planBegin", "policyEffective", "eligibility", "plan"],
        begin=True)
    term_date = _pick_date(
        pdi, ["eligibilityEnd", "planEnd", "policyExpiration", "eligibility", "plan"],
        begin=False)

    payer = data.get("payer") or {}
    payer_name = (payer.get("name") or payer.get("organizationName")
                  or req.payer_name or "Medicare")
    sub_resp = data.get("subscriber") or {}
    member_id = sub_resp.get("memberId") or (req.member_id or "").strip().upper()

    plan_name = ""
    for x in bi:
        if isinstance(x, dict) and x.get("planCoverage"):
            plan_name = x["planCoverage"]
            break

    if status == CoverageStatus.INACTIVE and term_date:
        status = CoverageStatus.TERMED

    return CoverageResult(
        status=status, source=source, payer_name=payer_name,
        payer_id=req.payer_id or "", plan_name=plan_name, member_id=member_id,
        effective_date=effective_date, term_date=term_date, benefit=benefit,
        prior_auth_required=prior_auth_required,
        confidence=1.0 if not errors else 0.0, errors=errors,
        raw={"configured": True, "benefit_count": len(bi),
             "stedi_id": data.get("id", ""),
             "x12_271": data.get("x12", "") or ""},
        trace=[f"{source}.parse_271"])


# ── helpers ──────────────────────────────────────────────────────────────────
def _pick_amount(items: list, code: str, field: str) -> Optional[float]:
    """Best value for a benefit `code` (prefer Individual + in-network)."""
    cands = [x for x in items if isinstance(x, dict)
             and str(x.get("code") or "") == code
             and x.get(field) not in (None, "")]
    if not cands:
        return None

    def score(x: dict) -> int:
        s = 0
        if (x.get("coverageLevelCode") or "") == "IND":
            s += 2
        if (x.get("inPlanNetworkIndicatorCode") or "") == "Y":
            s += 1
        return s

    cands.sort(key=score, reverse=True)
    return _f(cands[0].get(field))


def _pick_date(pdi: dict, keys: list, begin: bool) -> str:
    for k in keys:
        v = pdi.get(k)
        if v:
            d = _date_part(v, begin)
            if d:
                return d
    return ""


def _date_part(v, begin: bool) -> str:
    """Normalize a Stedi date (CCYYMMDD) or a CCYYMMDD-CCYYMMDD range."""
    if not v:
        return ""
    v = str(v).strip()
    if "-" in v and len(v) > 8:                # a compact range
        a, b = v.split("-", 1)
        return _fmt_date((a if begin else b).strip())
    return _fmt_date(v)


def _f(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
