"""AccessionGate — the pre-analytical decision the incumbents don't own.

At accession, BEFORE reagents are burned and a claim is generated, combine
    eligibility (hybrid engine) + medical necessity (LCD/NCD) + prior auth
into ONE disposition per ordered CPT, with the dollars and the denial risk.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .hybrid import HybridEligibilityEngine
from .intercept import run_intercept
from .models import CoverageResult, CoverageStatus, PatientRequest
from .policy import (check_medical_necessity, get_policy, is_prior_auth_required,
                     is_traditional_medicare)
from .prior_auth import PaStatus, PriorAuthEngine, PriorAuthRequest


class Disposition(str, Enum):
    CLEAR_TO_RUN = "CLEAR TO RUN"
    HOLD_PRIOR_AUTH = "HOLD — PRIOR AUTH"
    HOLD_MED_NECESSITY = "HOLD — MEDICAL NECESSITY"
    GET_ABN = "GET ABN"
    SELF_PAY = "SELF-PAY"
    DENY_RISK = "DENY RISK"


# Rollup severity (higher = more blocking) for the accession-level disposition.
_SEVERITY = {
    Disposition.CLEAR_TO_RUN: 0,
    Disposition.SELF_PAY: 1,
    Disposition.GET_ABN: 2,
    Disposition.HOLD_PRIOR_AUTH: 3,
    Disposition.DENY_RISK: 4,
    Disposition.HOLD_MED_NECESSITY: 5,
}

# Probability a line is ultimately paid, by disposition (for expected value).
_P_PAID = {
    Disposition.CLEAR_TO_RUN: 0.95,
    Disposition.HOLD_PRIOR_AUTH: 0.80,
    Disposition.GET_ABN: 0.55,
    Disposition.SELF_PAY: 0.30,
    Disposition.HOLD_MED_NECESSITY: 0.12,
    Disposition.DENY_RISK: 0.08,
}


@dataclass
class CptDisposition:
    cpt: str
    description: str
    disposition: Disposition
    coverage_status: str
    medically_necessary: bool
    prior_auth: PaStatus
    auth_number: str = ""
    pa_channel: str = ""
    allowed_amount: Optional[float] = None
    patient_responsibility: Optional[float] = None
    plan_pays: Optional[float] = None
    expected_value: Optional[float] = None
    reasons: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)


@dataclass
class AccessionResult:
    patient: str
    overall: Disposition
    coverage: CoverageResult
    lines: list[CptDisposition] = field(default_factory=list)
    trace: list[str] = field(default_factory=list)
    integrity: list[dict] = field(default_factory=list)  # MedPharma claim-integrity findings

    @property
    def total_expected_value(self) -> float:
        return round(sum(ln.expected_value or 0.0 for ln in self.lines), 2)

    def to_dict(self) -> dict:
        return {
            "patient": self.patient,
            "overall": self.overall.value,
            "total_expected_value": self.total_expected_value,
            "coverage": self.coverage.to_dict(),
            "trace": self.trace,
            "integrity": self.integrity,
            "lines": [{
                "cpt": ln.cpt, "description": ln.description,
                "disposition": ln.disposition.value,
                "coverage_status": ln.coverage_status,
                "medically_necessary": ln.medically_necessary,
                "prior_auth": ln.prior_auth.value, "auth_number": ln.auth_number,
                "pa_channel": ln.pa_channel, "allowed_amount": ln.allowed_amount,
                "patient_responsibility": ln.patient_responsibility,
                "plan_pays": ln.plan_pays, "expected_value": ln.expected_value,
                "reasons": ln.reasons, "actions": ln.actions,
                "integrity": [f for f in self.integrity
                              if ln.cpt == f.get("cpt") or ln.cpt in f.get("related", [])],
            } for ln in self.lines],
        }


class AccessionGate:
    def __init__(self, engine: HybridEligibilityEngine,
                 pa_engine: Optional[PriorAuthEngine] = None,
                 auto_submit_pa: bool = True):
        self.engine = engine
        self.pa_engine = pa_engine or PriorAuthEngine(sandbox=True)
        self.auto_submit_pa = auto_submit_pa

    def evaluate(self, req: PatientRequest) -> AccessionResult:
        coverage = self.engine.resolve(req)
        payer = coverage.payer_name or req.payer_name or ""
        is_medicare = is_traditional_medicare(payer)
        lines = [self._evaluate_cpt(cov, coverage, req, is_medicare)
                 for cov in coverage.per_cpt]
        overall = max((ln.disposition for ln in lines),
                      key=lambda d: _SEVERITY[d], default=Disposition.CLEAR_TO_RUN)
        integrity = run_intercept(req, coverage, lines)
        return AccessionResult(patient=req.full_name, overall=overall,
                               coverage=coverage, lines=lines, trace=coverage.trace,
                               integrity=integrity)

    def _evaluate_cpt(self, cov, coverage, req, is_medicare) -> CptDisposition:
        reasons: list[str] = []
        actions: list[str] = []
        pa_status = PaStatus.NOT_REQUIRED
        auth_number = pa_channel = ""

        active = coverage.status == CoverageStatus.ACTIVE
        mn = check_medical_necessity(cov.cpt, req.icd10_codes, coverage.payer_name, dos=req.dos)

        if not active:
            disp = Disposition.GET_ABN if is_medicare else Disposition.SELF_PAY
            reasons.append(f"Coverage {coverage.status.value}.")
            actions.append("Collect signed ABN + patient estimate." if is_medicare
                           else "Provide self-pay estimate; collect upfront.")
        elif not mn.necessary:
            disp = Disposition.GET_ABN if is_medicare else Disposition.HOLD_MED_NECESSITY
            reasons.append(mn.reason)
            actions.append("Query ordering provider for supporting Dx, or issue ABN.")
        elif not mn.frequency_ok:
            disp = Disposition.DENY_RISK
            reasons.append(mn.reason)
            actions.append("Confirm the repeat is warranted; attach documentation.")
        else:
            reasons.append(mn.reason)
            pa_required, pa_reason = is_prior_auth_required(cov.cpt, coverage.payer_name)
            if pa_required and self.auto_submit_pa:
                reasons.append(pa_reason)
                pares = self.pa_engine.submit(PriorAuthRequest(
                    cpt=cov.cpt, payer_name=coverage.payer_name,
                    member_id=coverage.member_id,
                    dx_codes=mn.matched_dx or req.icd10_codes,
                    provider_npi=req.provider_npi, dos=req.dos,
                    clinical_note="Symptoms documented; test ordered to guide therapy."))
                pa_status, auth_number, pa_channel = pares.status, pares.auth_number, pares.channel.value
                reasons.append(pares.determination)
                if pares.status == PaStatus.APPROVED:
                    disp = Disposition.CLEAR_TO_RUN
                    actions.append(f"Auth {auth_number} on file — release to bench.")
                elif pares.status == PaStatus.DENIED:
                    disp = Disposition.DENY_RISK
                    actions.append("Peer-to-peer review, or convert to ABN / self-pay.")
                else:
                    disp = Disposition.HOLD_PRIOR_AUTH
                    actions.append(f"PA {pares.status.value} via {pa_channel} ({pares.turnaround}).")
            elif pa_required:
                pa_status = PaStatus.REQUIRED
                disp = Disposition.HOLD_PRIOR_AUTH
                reasons.append(pa_reason)
                actions.append("Submit prior auth before running.")
            else:
                disp = Disposition.CLEAR_TO_RUN
                reasons.append(pa_reason)
                actions.append("Release to bench.")

        if mn.moldx_required:
            reasons.append("MolDX: DEX Z-code registration required.")
            actions.append("Attach DEX Z-code before claim submission.")

        return CptDisposition(
            cpt=cov.cpt, description=cov.description, disposition=disp,
            coverage_status=coverage.status.value, medically_necessary=mn.necessary,
            prior_auth=pa_status, auth_number=auth_number, pa_channel=pa_channel,
            allowed_amount=cov.allowed_amount, patient_responsibility=cov.patient_responsibility,
            plan_pays=cov.plan_pays,
            expected_value=self._expected_value(disp, cov.allowed_amount, cov.cpt),
            reasons=reasons, actions=actions)

    @staticmethod
    def _expected_value(disp: Disposition, allowed: Optional[float], cpt: str) -> float:
        pol = get_policy(cpt)
        cost = pol.cost_to_run if pol else 0.0
        return round((allowed or 0.0) * _P_PAID.get(disp, 0.3) - cost, 2)
