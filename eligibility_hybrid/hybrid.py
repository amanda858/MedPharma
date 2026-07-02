"""HybridEligibilityEngine — merges pVerify + Office Ally behind one call.

Default strategy = DISCOVER_THEN_VERIFY:
  • Known payer   -> pVerify.verify (rich REST benefits); fall back to Office
                     Ally 270/271 if pVerify errors or can't reach the payer.
  • Unknown payer -> discovery cascade (pVerify.discover, then Office Ally, the
                     stronger self-pay finder). On a hit, re-verify the found
                     payer through pVerify for full benefits.
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
    OFFICEALLY_FIRST = "officeally_first"


class HybridEligibilityEngine:
    def __init__(self, pverify: EligibilityProvider, officeally: EligibilityProvider,
                 strategy: HybridStrategy = HybridStrategy.DISCOVER_THEN_VERIFY):
        self.pverify = pverify
        self.officeally = officeally
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
    def _order(self) -> tuple[EligibilityProvider, EligibilityProvider]:
        if self.strategy == HybridStrategy.OFFICEALLY_FIRST:
            return self.officeally, self.pverify
        return self.pverify, self.officeally

    def _verify_known(self, req: PatientRequest, trace: list[str],
                      discovered: bool = False) -> Optional[CoverageResult]:
        primary, secondary = self._order()
        try:
            res = primary.verify(req)
            trace.append(f"{primary.name}.verify -> {res.status.value}")
            if res.status in (CoverageStatus.ACTIVE, CoverageStatus.TERMED):
                return res
        except Exception as e:  # provider-level failure -> fall back
            trace.append(f"{primary.name}.verify ERROR: {e}")
        try:
            res = secondary.verify(req)
            trace.append(f"{secondary.name}.verify (fallback) -> {res.status.value}")
            return res
        except Exception as e:
            trace.append(f"{secondary.name}.verify ERROR: {e}")
        return None

    def _discover(self, req: PatientRequest, trace: list[str]) -> Optional[CoverageResult]:
        for prov in (self.pverify, self.officeally):
            if not prov.supports_discovery():
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
