"""pVerify connector — real-time REST eligibility + insurance discovery.

MedPharma-authored connector to a third-party clearinghouse via that vendor's
PUBLISHED REST contract: OAuth2 client-credentials token -> Bearer ->
/API/EligibilitySummary and /API/InsuranceDiscovery. It contains no vendor
proprietary code; the vendor is a swappable data source behind MedPharma's own
`EligibilityProvider` interface. With no credentials it runs in SANDBOX mode and
returns deterministic MedPharma-authored mock responses so the whole engine works
today. Confirm exact field casing against the pVerify sandbox once credentials
are provisioned.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from datetime import date
from typing import Optional

from .models import (Benefit, CoverageResult, CoverageStatus, EligibilityProvider,
                     PatientRequest, ProviderError, stable_hash)

TOKEN_PATH = "/Token"
ELIG_PATH = "/API/EligibilitySummary"
DISCOVERY_PATH = "/API/InsuranceDiscovery"


class PVerifyProvider(EligibilityProvider):
    name = "pverify"

    def __init__(self, client_id: str = "", client_secret: str = "",
                 base_url: str = "https://api.pverify.com", sandbox: bool = True,
                 timeout: int = 30):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self.sandbox = sandbox or not (client_id and client_secret)
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_exp = 0.0

    def supports_discovery(self) -> bool:
        return True

    @property
    def configured(self) -> bool:
        """True only when real creds are set AND sandbox is off, so a
        configured pVerify always performs a REAL call - it never returns a
        mock/fabricated result on the live verify path."""
        return bool(self.client_id and self.client_secret and not self.sandbox)

    # ── live HTTP plumbing (exercised only with real credentials) ──
    def _get_token(self) -> str:
        if self._token and time.time() < self._token_exp - 30:
            return self._token
        data = urllib.parse.urlencode({
            "Client_Id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }).encode()
        req = urllib.request.Request(
            self.base_url + TOKEN_PATH, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                body = json.loads(r.read().decode())
        except Exception as e:  # pragma: no cover - network
            raise ProviderError(self.name, f"token request failed: {e}", retryable=True)
        self._token = body.get("access_token")
        self._token_exp = time.time() + int(body.get("expires_in", 3600))
        if not self._token:
            raise ProviderError(self.name, "no access_token in token response")
        return self._token

    def _post(self, path: str, payload: dict) -> dict:
        token = self._get_token()
        req = urllib.request.Request(
            self.base_url + path, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {token}",
                     "Client-API-Id": self.client_id})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # pragma: no cover - network
            raise ProviderError(self.name, f"POST {path} failed: {e}", retryable=True)

    def _elig_payload(self, req: PatientRequest) -> dict:
        dos = date.fromisoformat(req.dos).strftime("%m/%d/%Y")
        return {
            "payerCode": req.payer_id or "",
            "payerName": req.payer_name or "",
            "provider": {"npi": req.provider_npi, "lastName": req.provider_name},
            "subscriber": {
                "firstName": req.first_name,
                "lastName": req.last_name,
                "dob": date.fromisoformat(req.dob).strftime("%m/%d/%Y"),
                "memberID": req.member_id or "",
            },
            "isSubscriberPatient": "True",
            "doS_StartDate": dos,
            "doS_EndDate": dos,
            "serviceCodes": ",".join(req.service_type_codes or ["30"]),
            "includeTextResponse": False,
        }

    # ── public API ──
    def verify(self, req: PatientRequest) -> CoverageResult:
        if self.sandbox:
            return self._mock_verify(req)
        return self._map_elig(self._post(ELIG_PATH, self._elig_payload(req)), req)

    def discover(self, req: PatientRequest) -> Optional[CoverageResult]:
        if self.sandbox:
            return self._mock_discover(req)
        payload = {
            "subscriber": {
                "firstName": req.first_name, "lastName": req.last_name,
                "dob": date.fromisoformat(req.dob).strftime("%m/%d/%Y"),
                "ssn": req.ssn_last4 or "",
            },
            "provider": {"npi": req.provider_npi},
            "doS_StartDate": date.fromisoformat(req.dos).strftime("%m/%d/%Y"),
        }
        res = self._map_elig(self._post(DISCOVERY_PATH, payload), req)
        if res and res.status == CoverageStatus.ACTIVE:
            res.discovered = True
            return res
        return None

    # ── live response mapping ──
    def _map_elig(self, raw: dict, req: PatientRequest) -> CoverageResult:
        pcs = raw.get("planCoverageSummary", {}) or {}
        hbs = raw.get("hbpc_Deductible_OOP_Summary", {}) or {}
        status_txt = str(pcs.get("status", raw.get("status", ""))).lower()
        status = (CoverageStatus.ACTIVE if "active" in status_txt
                  else CoverageStatus.INACTIVE if status_txt else CoverageStatus.UNKNOWN)
        benefit = Benefit(
            copay=_money(pcs.get("copay")),
            deductible_total=_money(hbs.get("individualDeductibleInNet")),
            deductible_met=_money(hbs.get("individualDeductibleMetInNet")),
            coinsurance_pct=_pct(pcs.get("coInsurance")),
            oop_total=_money(hbs.get("individualOOPInNet")),
            oop_met=_money(hbs.get("individualOOPMetInNet")),
        )
        return CoverageResult(
            status=status, source=self.name,
            payer_name=pcs.get("payerName", req.payer_name or ""),
            payer_id=pcs.get("payerCode", req.payer_id or ""),
            plan_name=pcs.get("planName", ""),
            member_id=raw.get("memberID", req.member_id or ""),
            effective_date=pcs.get("effectiveDate", ""),
            term_date=pcs.get("expiryDate", ""),
            benefit=benefit, raw=raw, trace=[f"{self.name}.verify"],
        )

    # ── deterministic sandbox mocks ──
    def _mock_verify(self, req: PatientRequest) -> CoverageResult:
        h = stable_hash(req.member_id or req.full_name, req.payer_name or "")
        termed = (req.member_id or "").upper().startswith("U88") or h % 19 == 0
        ded_total = [500, 1000, 1500, 2500, 5000][h % 5]
        benefit = Benefit(
            copay=[0, 10, 20, 25, 35][h % 5],
            deductible_total=float(ded_total),
            deductible_met=round(ded_total * (h % 100) / 100.0, 2),
            coinsurance_pct=float([0, 10, 20, 30][(h >> 2) % 4]),
            oop_total=float([3000, 6000, 8500][h % 3]),
            oop_met=float([200, 1400, 3000][(h >> 3) % 3]),
        )
        return CoverageResult(
            status=CoverageStatus.TERMED if termed else CoverageStatus.ACTIVE,
            source=self.name,
            payer_name=req.payer_name or "Discovered Payer",
            payer_id=req.payer_id or str(h % 90000 + 10000),
            plan_name=(req.payer_name or "Commercial") + " PPO",
            member_id=req.member_id or f"PV{h % 1000000:06d}",
            effective_date="01/01/2026",
            term_date="05/31/2026" if termed else "",
            benefit=benefit, confidence=0.95, raw={"mock": True},
            trace=[f"{self.name}.verify(sandbox)"],
        )

    def _mock_discover(self, req: PatientRequest) -> Optional[CoverageResult]:
        # pVerify discovery hits on strong demographic matches; defers self-pay
        # SSN-only cases to the secondary discovery provider.
        h = stable_hash(req.full_name, req.dob)
        if req.payer_known or req.ssn_last4 or h % 5 != 0:
            return None
        res = self._mock_verify(req)
        res.discovered = True
        res.payer_name, res.payer_id = "UnitedHealthcare", "87726"
        res.confidence = 0.72
        res.trace = [f"{self.name}.discover(sandbox)"]
        return res


def _money(v) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace("$", "").replace(",", "").strip())
    except ValueError:
        return None


def _pct(v) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace("%", "").strip())
    except ValueError:
        return None
