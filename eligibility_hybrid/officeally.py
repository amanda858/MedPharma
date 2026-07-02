"""Office Ally adapter — X12 270/271 real-time eligibility + Insurance Discovery.

Office Ally's clearinghouse reaches 6,000+ payers via 270/271 (SFTP, API, SOAP,
MIME). Insurance Discovery finds active billable coverage on self-pay accounts
(10-30% hit rate) — the piece that solves "we don't know the payer".

Sandbox mode returns deterministic simulated responses. The live real-time path
builds a 270 and parses the returned 271; live Insurance Discovery is a batch
workflow and is left as a clearly-marked integration point.
"""
from __future__ import annotations

import base64
from datetime import datetime
from typing import Optional

from .models import (Benefit, CoverageResult, CoverageStatus, EligibilityProvider,
                     PatientRequest, ProviderError, stable_hash)


class OfficeAllyProvider(EligibilityProvider):
    name = "officeally"

    def __init__(self, username: str = "", password: str = "", sender_id: str = "",
                 realtime_url: str = "", sandbox: bool = True, timeout: int = 30):
        self.username = username
        self.password = password
        self.sender_id = sender_id or "OFFICEALLY"
        self.realtime_url = realtime_url
        self.sandbox = sandbox or not (username and password and realtime_url)
        self.timeout = timeout

    def supports_discovery(self) -> bool:
        return True

    # ── public API ─────────────────────────────────────────────────────────
    def verify(self, req: PatientRequest) -> CoverageResult:
        if self.sandbox:
            return self._mock_verify(req)
        resp = self._post_realtime(build_270(req, self.sender_id))
        return parse_271(resp, req, self.name)

    def discover(self, req: PatientRequest) -> Optional[CoverageResult]:
        if self.sandbox:
            return self._mock_discover(req)
        raise ProviderError(
            self.name, "live Insurance Discovery is a batch workflow — configure the "
                       "SFTP/batch endpoint before enabling in production")

    def _post_realtime(self, x12_270: str) -> str:
        import urllib.request
        req = urllib.request.Request(
            self.realtime_url, data=x12_270.encode(),
            headers={"Content-Type": "application/edi-x12",
                     "Authorization": "Basic " + base64.b64encode(
                         f"{self.username}:{self.password}".encode()).decode()})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return r.read().decode()
        except Exception as e:  # pragma: no cover - network
            raise ProviderError(self.name, f"real-time 270 failed: {e}", retryable=True)

    # ── deterministic sandbox mocks ─────────────────────────────────────────
    def _mock_verify(self, req: PatientRequest) -> CoverageResult:
        h = stable_hash(req.member_id or req.full_name, req.payer_name or "")
        ded_total = [500, 1500, 3000][h % 3]
        benefit = Benefit(
            copay=[0, 20, 40][h % 3],
            deductible_total=ded_total,
            deductible_met=round(ded_total * (h % 90) / 100.0, 2),
            coinsurance_pct=[0, 20, 30][(h >> 1) % 3],
            oop_total=[4000, 7000][h % 2],
            oop_met=[500, 2000][(h >> 2) % 2],
        )
        return CoverageResult(
            status=CoverageStatus.ACTIVE, source=self.name,
            payer_name=req.payer_name or "Payer (271)",
            payer_id=req.payer_id or str(h % 9000 + 1000),
            plan_name=(req.payer_name or "Plan") + " (271)",
            member_id=req.member_id or f"OA{h % 1000000:06d}",
            effective_date="01/01/2026", benefit=benefit, confidence=0.90,
            raw={"mock271": True}, trace=[f"{self.name}.verify(sandbox 270/271)"],
        )

    def _mock_discover(self, req: PatientRequest) -> Optional[CoverageResult]:
        # Office Ally's edge: self-pay discovery incl. Medicaid/MCO. SSN sharply
        # raises the hit rate; otherwise ~30% (matches their published 10-30%).
        if req.payer_known:
            return None
        h = stable_hash(req.full_name, req.dob, req.zip_code or "")
        if not req.ssn_last4 and h % 10 >= 3:
            return None
        payer, pid = [("Florida Medicaid", "FLMCD"), ("Humana", "61101"),
                      ("Aetna Better Health", "128FL"), ("Medicare Part B", "MCARE")][h % 4]
        return CoverageResult(
            status=CoverageStatus.ACTIVE, source=self.name,
            payer_name=payer, payer_id=pid, plan_name=payer + " (discovered)",
            member_id=f"D{h % 100000000:08d}", effective_date="01/01/2026",
            benefit=Benefit(copay=0, deductible_total=0, deductible_met=0,
                            coinsurance_pct=0, oop_total=0, oop_met=0),
            discovered=True, confidence=0.68, raw={"discovery": True},
            trace=[f"{self.name}.discover(sandbox)"],
        )


# ── minimal X12 270 builder / 271 parser (real-time path) ───────────────────
def build_270(req: PatientRequest, sender_id: str, receiver_id: str = "OFFICEALLY") -> str:
    now = datetime.now()
    d8, tm = now.strftime("%Y%m%d"), now.strftime("%H%M")
    ctrl = f"{stable_hash(req.full_name, d8) % 1000000000:09d}"
    segs = [
        f"ISA*00*          *00*          *ZZ*{sender_id:<15}*ZZ*{receiver_id:<15}"
        f"*{now.strftime('%y%m%d')}*{tm}*^*00501*{ctrl}*0*P*:",
        f"GS*HS*{sender_id}*{receiver_id}*{d8}*{tm}*1*X*005010X279A1",
        "ST*270*0001*005010X279A1",
        f"BHT*0022*13*{ctrl}*{d8}*{tm}",
        "HL*1**20*1",
        f"NM1*PR*2*{req.payer_name or 'PAYER'}*****PI*{req.payer_id or ''}",
        "HL*2*1*21*1",
        f"NM1*1P*2*{req.provider_name or 'LAB'}*****XX*{req.provider_npi or ''}",
        "HL*3*2*22*0",
        f"TRN*1*{ctrl}*{req.provider_npi or '1'}",
        f"NM1*IL*1*{req.last_name}*{req.first_name}****MI*{req.member_id or ''}",
        f"DMG*D8*{req.dob.replace('-', '')}*{req.gender or 'U'}",
        f"DTP*291*D8*{req.dos.replace('-', '')}",
        f"EQ*{(req.service_type_codes or ['30'])[0]}",
        "SE*13*0001", "GE*1*1", f"IEA*1*{ctrl}",
    ]
    return "~".join(segs) + "~"


def parse_271(edi: str, req: PatientRequest, source: str) -> CoverageResult:
    segs = [s.strip() for s in edi.replace("\n", "").split("~") if s.strip()]
    status = CoverageStatus.UNKNOWN
    benefit = Benefit()
    payer_name, member_id = req.payer_name or "", req.member_id or ""
    for seg in segs:
        el = seg.split("*")
        tag = el[0]
        if tag == "EB" and len(el) > 1:
            code = el[1]
            if code == "1":
                status = CoverageStatus.ACTIVE
            elif code in ("6", "7", "8"):
                status = CoverageStatus.INACTIVE
            elif code == "B" and len(el) > 7:
                benefit.copay = _f(el[7])
            elif code == "C" and len(el) > 7:
                benefit.deductible_total = _f(el[7])
            elif code == "A" and len(el) > 8:
                val = _f(el[8])
                if val is not None:
                    benefit.coinsurance_pct = val * 100 if val <= 1 else val
        elif tag == "NM1" and len(el) > 3 and el[1] == "PR":
            payer_name = el[3] or payer_name
    return CoverageResult(status=status, source=source, payer_name=payer_name,
                          member_id=member_id, benefit=benefit,
                          raw={"x12_271_segments": len(segs)},
                          trace=[f"{source}.parse_271"])


def _f(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
