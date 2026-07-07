"""HybridEligibilityEngine — a primary + an optional secondary provider behind
one call.

Default strategy = DISCOVER_THEN_VERIFY:
  • Known payer   -> primary.verify (rich REST benefits); fall back to the
                     secondary provider's 270/271 if the primary errors or
                     can't reach the payer.
  • Unknown payer -> discovery cascade over whichever providers support it. On
                     a hit, re-verify the found payer through the primary for
                     full benefits.
  • Always normalize to per-CPT coverage + patient responsibility.

Every step is appended to `result.trace`, so you can see exactly what ran.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from .models import CoverageResult, CoverageStatus, EligibilityProvider, PatientRequest
from .normalize import enrich_cpt_coverage


class HybridStrategy(str, Enum):
    DISCOVER_THEN_VERIFY = "discover_then_verify"
    PVERIFY_FIRST = "pverify_first"
    SECONDARY_FIRST = "secondary_first"


class HybridEligibilityEngine:
    def __init__(self, pverify: EligibilityProvider,
                 secondary: Optional[EligibilityProvider] = None,
                 strategy: HybridStrategy = HybridStrategy.DISCOVER_THEN_VERIFY):
        self.pverify = pverify
        self.secondary = secondary
        self.strategy = strategy

    def resolve(self, req: PatientRequest) -> CoverageResult:
        trace: list[str] = []
        result: Optional[CoverageResult] = None

        if req.payer_known:
            result = self._verify_known(req, trace)
        else:
            trace.append("payer unknown -> discovery cascade")
            discovered = self._discover(req, trace)
            if discovered is not None:
                # Adopt the discovered identity, then re-verify for rich benefits.
                req.payer_name = discovered.payer_name
                req.payer_id = discovered.payer_id
                req.member_id = discovered.member_id or req.member_id
                verified = self._verify_known(req, trace, discovered=True)
                if verified is not None and verified.status == CoverageStatus.ACTIVE:
                    verified.discovered = True
                    verified.confidence = min(discovered.confidence, verified.confidence)
                    result = verified
                else:
                    result = discovered

        if result is None:
            result = CoverageResult(
                status=CoverageStatus.UNKNOWN, source="hybrid",
                errors=["No active coverage found by any provider."])

        result.source = "hybrid"
        result.trace = trace + result.trace
        enrich_cpt_coverage(result, req)
        return result

    # ── helpers ─────────────────────────────────────────────────────────────
    def _order(self) -> tuple[EligibilityProvider, ...]:
        if self.secondary is None:
            return (self.pverify,)
        if self.strategy == HybridStrategy.SECONDARY_FIRST:
            return self.secondary, self.pverify
        return self.pverify, self.secondary

    def _verify_known(self, req: PatientRequest, trace: list[str],
                      discovered: bool = False) -> Optional[CoverageResult]:
        last: Optional[CoverageResult] = None
        for i, prov in enumerate(self._order()):
            tag = "" if i == 0 else " (fallback)"
            try:
                res = prov.verify(req)
                trace.append(f"{prov.name}.verify{tag} -> {res.status.value}")
                if res.status in (CoverageStatus.ACTIVE, CoverageStatus.TERMED):
                    return res
                last = res
            except Exception as e:  # provider-level failure -> try the next
                trace.append(f"{prov.name}.verify{tag} ERROR: {e}")
        return last

    def _discover(self, req: PatientRequest, trace: list[str]) -> Optional[CoverageResult]:
        for prov in (self.pverify, self.secondary):
            if prov is None or not prov.supports_discovery():
                continue
            try:
                res = prov.discover(req)
            except Exception as e:
                trace.append(f"{prov.name}.discover ERROR: {e}")
                continue
            if res is not None:
                trace.append(
                    f"{prov.name}.discover -> HIT {res.payer_name} ({res.confidence:.0%})")
                return res
            trace.append(f"{prov.name}.discover -> no coverage found")
        return None
