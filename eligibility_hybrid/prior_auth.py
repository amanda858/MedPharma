"""Prior authorization — determination + submission (278 / FHIR PAS / portal RPA).

Three submission channels mirror the real world:
  • X12 278        HIPAA auth request via clearinghouse
  • FHIR Da Vinci PAS   CMS-0057-F mandated (~2027) for payers with APIs
  • Portal / RPA   agentic fallback where the payer exposes no API (human-in-loop)

Sandbox mode returns deterministic decisions so the flow runs today: a request
with a supporting Dx *and* clinical documentation is approved; with a Dx but no
docs it pends; with no Dx it is denied. Live channels are marked integration
points.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .models import stable_hash


class PaStatus(str, Enum):
    NOT_REQUIRED = "Not Required"
    REQUIRED = "Required"
    PENDING = "Pending"
    APPROVED = "Approved"
    DENIED = "Denied"


class PaChannel(str, Enum):
    X12_278 = "X12 278"
    FHIR_PAS = "FHIR Da Vinci PAS"
    PORTAL_RPA = "Portal / RPA"
    NONE = "None"


@dataclass
class PriorAuthRequest:
    cpt: str
    payer_name: str
    member_id: str
    dx_codes: list[str] = field(default_factory=list)
    provider_npi: str = ""
    dos: str = ""
    clinical_note: str = ""


@dataclass
class PriorAuthResult:
    cpt: str
    status: PaStatus
    channel: PaChannel = PaChannel.NONE
    auth_number: str = ""
    determination: str = ""
    turnaround: str = ""
    trace: list[str] = field(default_factory=list)


_FHIR_PAYERS = ("unitedhealthcare", "uhc", "cigna", "aetna", "humana", "anthem")
_API_278_PAYERS = ("medicaid", "better health", "wellcare", "molina", "bcbs", "blue")

_TURNAROUND = {
    PaChannel.FHIR_PAS: "real-time / 72h",
    PaChannel.X12_278: "72h urgent / 7d standard",
    PaChannel.PORTAL_RPA: "1-3 business days",
}


class PriorAuthEngine:
    def __init__(self, sandbox: bool = True, timeout: int = 30):
        self.sandbox = sandbox
        self.timeout = timeout

    def choose_channel(self, payer_name: str) -> PaChannel:
        p = (payer_name or "").lower()
        if any(h in p for h in _FHIR_PAYERS):
            return PaChannel.FHIR_PAS
        if any(h in p for h in _API_278_PAYERS):
            return PaChannel.X12_278
        return PaChannel.PORTAL_RPA

    def submit(self, req: PriorAuthRequest) -> PriorAuthResult:
        channel = self.choose_channel(req.payer_name)
        if self.sandbox:
            return self._mock_submit(req, channel)
        raise NotImplementedError(
            f"live PA submission via {channel.value} is an integration point")

    def _mock_submit(self, req: PriorAuthRequest, channel: PaChannel) -> PriorAuthResult:
        turnaround = _TURNAROUND.get(channel, "")
        trace = [f"determine PA required -> submit via {channel.value}"]
        if not req.dx_codes:
            return PriorAuthResult(
                req.cpt, PaStatus.DENIED, channel,
                determination="Denied — no supporting diagnosis submitted.",
                turnaround=turnaround, trace=trace + ["no Dx -> denied"])
        if not req.clinical_note:
            return PriorAuthResult(
                req.cpt, PaStatus.PENDING, channel,
                determination="Submitted; pended for clinical documentation.",
                turnaround=turnaround, trace=trace + ["missing clinical note -> pending"])
        auth = f"AUTH{stable_hash(req.cpt, req.member_id, req.payer_name) % 1000000:06d}"
        return PriorAuthResult(
            req.cpt, PaStatus.APPROVED, channel, auth_number=auth,
            determination=f"Approved via {channel.value}; medical necessity met.",
            turnaround=turnaround, trace=trace + [f"approved {auth}"])
