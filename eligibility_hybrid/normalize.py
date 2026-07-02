"""Lab-specific normalization: per-CPT coverage + patient responsibility.

This is the layer that makes the engine *better* than a raw vendor response —
given plan benefits and the ordered lab CPTs, it answers "is this covered, does
it need prior auth, and who owes what" BEFORE the sample is run.
"""
from __future__ import annotations

from typing import Optional

from .models import (Benefit, CoverageResult, CoverageStatus, CptCoverage,
                     CptStatus, PatientRequest, stable_hash)

# Minimal lab policy/fee reference (mock allowed amounts + typical PA defaults).
# Replace `allowed` with your real contracted/fee-schedule amounts per payer.
LAB_CPTS: dict[str, dict] = {
    "87631": {"desc": "Respiratory pathogen panel, 12-25 targets (PCR)", "allowed": 416.78, "pa": True},
    "87633": {"desc": "Respiratory pathogen panel, 22+ targets (PCR)",  "allowed": 518.32, "pa": True},
    "87507": {"desc": "GI pathogen panel, 12-25 targets (PCR)",         "allowed": 385.41, "pa": True},
    "87798": {"desc": "UTI / urogenital pathogen, each organism (PCR)", "allowed": 148.09, "pa": False},
    "87635": {"desc": "SARS-CoV-2 (COVID-19), amplified probe",         "allowed": 51.31,  "pa": False},
    "80305": {"desc": "Drug test(s), presumptive",                      "allowed": 79.42,  "pa": False},
    "88305": {"desc": "Surgical pathology, level IV",                   "allowed": 71.66,  "pa": False},
    "81479": {"desc": "Unlisted molecular pathology procedure",         "allowed": 597.00, "pa": True},
}


def estimate_patient_responsibility(benefit: Benefit, allowed: float) -> tuple[float, float, list[str]]:
    """Return (patient_owes, plan_pays, explanation_lines) for one line item."""
    lines: list[str] = []
    copay = benefit.copay or 0.0
    ded_remaining = benefit.deductible_remaining or 0.0
    to_ded = min(allowed, ded_remaining) if ded_remaining else 0.0
    after_ded = max(0.0, allowed - to_ded)
    coins_pct = benefit.coinsurance_pct or 0.0
    coins_amt = round(after_ded * coins_pct / 100.0, 2)
    patient = round(copay + to_ded + coins_amt, 2)
    if benefit.oop_remaining is not None:
        patient = min(patient, benefit.oop_remaining)
    patient = min(patient, allowed)
    plan_pays = round(max(0.0, allowed - patient), 2)
    if copay:
        lines.append(f"copay ${copay:,.2f}")
    if to_ded:
        lines.append(f"deductible ${to_ded:,.2f} of ${ded_remaining:,.2f} left")
    if coins_amt:
        lines.append(f"coinsurance {coins_pct:.0f}% = ${coins_amt:,.2f}")
    if not lines:
        lines.append("no member cost share")
    return patient, plan_pays, lines


def enrich_cpt_coverage(result: CoverageResult, req: PatientRequest) -> CoverageResult:
    """Populate `result.per_cpt` for each ordered CPT using benefit + policy."""
    result.per_cpt = []
    not_active = result.status != CoverageStatus.ACTIVE
    for cpt in (req.cpt_codes or []):
        meta = LAB_CPTS.get(cpt, {"desc": "Unlisted lab procedure", "allowed": 0.0, "pa": False})
        cov = CptCoverage(cpt=cpt, description=meta["desc"], allowed_amount=meta["allowed"])
        if not_active:
            cov.status = CptStatus.NOT_COVERED
            cov.patient_responsibility = meta["allowed"]
            cov.plan_pays = 0.0
            cov.note = "Coverage not active — claim will deny; do not run on assignment."
        elif stable_hash(cpt, result.payer_id or result.payer_name) % 17 == 0:
            cov.status = CptStatus.NOT_COVERED
            cov.patient_responsibility = meta["allowed"]
            cov.plan_pays = 0.0
            cov.note = "Payer policy: experimental / non-covered for this plan."
        else:
            pa = bool(meta["pa"]) or bool(result.prior_auth_required)
            cov.status = CptStatus.PRIOR_AUTH if pa else CptStatus.COVERED
            patient, plan_pays, expl = estimate_patient_responsibility(result.benefit, meta["allowed"])
            cov.patient_responsibility = patient
            cov.plan_pays = plan_pays
            cov.note = ("prior auth required; " if pa else "") + "; ".join(expl)
        result.per_cpt.append(cov)
    if result.prior_auth_required is None:
        result.prior_auth_required = any(c.status == CptStatus.PRIOR_AUTH for c in result.per_cpt)
    return result
