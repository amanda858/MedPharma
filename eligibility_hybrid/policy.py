"""Lab medical-necessity + prior-auth policy engine (LCD/NCD-style).

This is the "brain": payer/CPT rules that decide medical necessity, prior-auth
requirement, frequency limits, and MolDX registration for lab tests. The tables
here are representative STARTER policy — replace with your contracted LCD/NCD +
payer policy data as you build out.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class CptPolicy:
    cpt: str
    description: str
    covered_icd10_prefixes: list[str]      # Dx families that support necessity
    prior_auth_required: bool = False
    frequency_limit_days: Optional[int] = None
    moldx_zcode: Optional[str] = None      # molecular tests -> DEX/Z-code
    cost_to_run: float = 0.0               # reagent + labor (for expected value)
    notes: str = ""


POLICIES: dict[str, CptPolicy] = {
    "87631": CptPolicy(
        "87631", "Respiratory pathogen panel, 12-25 targets (PCR)",
        ["J", "U07", "R05", "R06", "B97", "A37", "Z20.8"],
        prior_auth_required=True, frequency_limit_days=14, cost_to_run=120.0,
        notes="Large multiplex respiratory panels: many commercial plans require PA; frequency-limited."),
    "87633": CptPolicy(
        "87633", "Respiratory pathogen panel, 22+ targets (PCR)",
        ["J", "U07", "R05", "R06", "B97"],
        prior_auth_required=True, frequency_limit_days=14, cost_to_run=140.0),
    "87507": CptPolicy(
        "87507", "GI pathogen panel, 12-25 targets (PCR)",
        ["A0", "A08", "A09", "R19.7", "K52", "K59.1"],
        prior_auth_required=True, frequency_limit_days=14, cost_to_run=110.0,
        notes="GI panels commonly require PA + diarrhea/enteritis Dx."),
    "87798": CptPolicy(
        "87798", "UTI / urogenital pathogen, each organism (PCR)",
        ["N39", "N30", "R30", "R82", "N34", "O23"],
        prior_auth_required=False, cost_to_run=45.0),
    "87635": CptPolicy(
        "87635", "SARS-CoV-2 (COVID-19), amplified probe",
        ["U07", "Z20.822", "J12.82", "R05", "R06.0"],
        prior_auth_required=False, cost_to_run=20.0),
    "80305": CptPolicy(
        "80305", "Drug test(s), presumptive",
        ["F1", "Z79", "T40", "R78"],
        prior_auth_required=False, frequency_limit_days=1, cost_to_run=15.0),
    "88305": CptPolicy(
        "88305", "Surgical pathology, level IV",
        [], prior_auth_required=False, cost_to_run=25.0,
        notes="No Dx restriction; medical necessity by specimen."),
    "81479": CptPolicy(
        "81479", "Unlisted molecular pathology procedure",
        ["C", "D", "Z15", "Z80"],
        prior_auth_required=True, moldx_zcode="REQUIRED", cost_to_run=180.0,
        notes="MolDX: DEX Z-code registration required for Medicare / MolDX payers."),
}

_MEDICARE_HINTS = ("medicare", "cms", "part b")
_ADVANTAGE_HINTS = ("advantage", "medicare advantage")


@dataclass
class MedNecResult:
    cpt: str
    necessary: bool
    matched_dx: list[str] = field(default_factory=list)
    frequency_ok: bool = True
    moldx_required: bool = False
    reason: str = ""


def get_policy(cpt: str) -> Optional[CptPolicy]:
    return POLICIES.get((cpt or "").strip())


def is_traditional_medicare(payer_name: str) -> bool:
    p = (payer_name or "").lower()
    return any(h in p for h in _MEDICARE_HINTS) and not any(h in p for h in _ADVANTAGE_HINTS)


def _norm(code: str) -> str:
    return (code or "").strip().upper().replace(".", "")


def check_medical_necessity(cpt: str, icd10_codes: list[str], payer_name: str = "",
                            prior_dates: Optional[list[str]] = None,
                            dos: Optional[str] = None) -> MedNecResult:
    pol = get_policy(cpt)
    codes = [c.strip().upper() for c in (icd10_codes or []) if c.strip()]
    if pol is None:
        return MedNecResult(cpt, True, codes, reason="No LCD/NCD policy on file — manual review.")

    if pol.covered_icd10_prefixes:
        prefixes = [_norm(p) for p in pol.covered_icd10_prefixes]
        matched = [c for c in codes if any(_norm(c).startswith(p) for p in prefixes)]
        necessary = bool(matched)
    else:
        matched, necessary = codes, True

    frequency_ok = True
    if pol.frequency_limit_days and prior_dates and dos:
        try:
            d0 = date.fromisoformat(dos)
            for pd in prior_dates:
                if abs((d0 - date.fromisoformat(pd)).days) < pol.frequency_limit_days:
                    frequency_ok = False
                    break
        except ValueError:
            pass

    moldx_required = bool(pol.moldx_zcode) and is_traditional_medicare(payer_name)

    if not necessary:
        needs = ", ".join(pol.covered_icd10_prefixes[:4])
        reason = (f"Dx {', '.join(codes) or '(none)'} not supported for {cpt}; "
                  f"needs {needs}…")
    elif not frequency_ok:
        reason = f"Frequency: {cpt} repeated within {pol.frequency_limit_days} days."
    elif moldx_required:
        reason = f"Medically necessary, but MolDX Z-code registration required for {cpt}."
    else:
        reason = f"Medically necessary: Dx {', '.join(matched) or 'n/a'} supports {cpt}."
    return MedNecResult(cpt, necessary, matched, frequency_ok, moldx_required, reason)


def is_prior_auth_required(cpt: str, payer_name: str, plan_name: str = "") -> tuple[bool, str]:
    pol = get_policy(cpt)
    if pol is None:
        return False, "No policy on file — confirm PA manually."
    if is_traditional_medicare(payer_name):
        return False, "Traditional Medicare: no PA for clinical lab (ABN / medical necessity governs)."
    if pol.prior_auth_required:
        return True, f"{payer_name or 'Payer'} requires PA for {cpt} ({pol.description})."
    return False, f"No PA required for {cpt} under {payer_name or 'payer'} policy."
