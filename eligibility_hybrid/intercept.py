"""MedPharma Claim-Integrity Intercept — proprietary, independently invented.

This is MedPharma intellectual property. It is an ORIGINAL implementation of
publicly published correct-coding and coverage-integrity processes — it contains
no third-party vendor code and copies no proprietary algorithm. Every rule cites
the PUBLIC standard it derives from (CMS NCCI/MUE quarterly files, CMS CLIA/QW
policy, X12 271 plan-date semantics, Palmetto GBA MolDX LCDs). Those standards
are facts and government rules, free for anyone to implement independently.

Design lineage: the same deterministic, transparent "rule intercept" pattern used
elsewhere in the platform (rule_intercept.py). Every decision is explainable —
each finding carries a machine code, a severity, a plain-English message, the
recommended action, and the public BASIS it rests on. Nothing is a black box.

Why it exists: no eligibility/verification system in the market is 100% accurate.
Incumbents check "is the member active" and stop. They routinely miss the coding
integrity that decides whether a technically-covered test is actually PAID:
component bundling (NCCI PTP), unit caps (MUE), duplicate ordering, waived-test
modifiers (QW), retroactive termination as-of the date of service, and molecular
registration (MolDX). Catching these BEFORE the sample runs is the differentiator.

STARTER TABLES: the edit tables below are a representative, clearly-labeled
starter set so the engine works today in sandbox. In production they are loaded
from the current CMS NCCI PTP and MUE quarterly files (public) plus contracted
payer policy — not hand-maintained here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from .models import CoverageResult, CoverageStatus, PatientRequest
from .policy import get_policy, is_traditional_medicare

PRODUCT_NAME = "MedPharma Coverage Intercept"
ENGINE_NAME = "MedPharma Claim-Integrity Intercept"
METHOD = "medpharma-intercept"

# Severity ladder (higher = more blocking) — mirrors the rule-intercept style.
SEV_INFO = "info"
SEV_ADVISORY = "advisory"
SEV_WARN = "warn"
SEV_BLOCK = "block"
_SEV_RANK = {SEV_INFO: 0, SEV_ADVISORY: 1, SEV_WARN: 2, SEV_BLOCK: 3}

# ── PUBLIC-STANDARD STARTER EDIT TABLES ────────────────────────────────────
# CMS NCCI Procedure-to-Procedure (PTP): column-1 (comprehensive) code -> set of
# column-2 codes that are bundled into it and NOT separately payable unless an
# appropriate distinct-service modifier (59 / XE / XS / XP / XU) is documented.
# Source: CMS National Correct Coding Initiative PTP edits (public quarterly file).
_NCCI_PTP: dict[str, set[str]] = {
    # Large multiplex respiratory panels subsume the single-target assays.
    "87631": {"87635", "87636", "87637", "87798"},
    "87633": {"87631", "87635", "87636", "87637", "87798"},
}
_PTP_BYPASS_MODIFIERS = ("59", "XE", "XS", "XP", "XU")

# CMS Medically Unlikely Edits (MUE): max units allowed per code per date of
# service. Source: CMS MUE (public quarterly file). None = no starter cap on file.
_MUE_MAX_UNITS: dict[str, int] = {
    "87631": 1, "87633": 1, "87507": 1, "87635": 1, "80305": 1, "81479": 1,
    "87798": 3, "88305": 10,
}

# CLIA-waived tests that require the QW modifier when billed to Medicare.
# Source: CMS CLIA-waived test list; QW modifier policy (CMS Pub 100-04, ch. 16).
_CLIA_WAIVED_QW: set[str] = {"87635", "80305", "87804", "87880", "81002", "82962"}

_BASIS_NCCI = "CMS National Correct Coding Initiative (NCCI) PTP edits — public quarterly file."
_BASIS_MUE = "CMS Medically Unlikely Edits (MUE) — public quarterly file."
_BASIS_QW = "CMS CLIA-waived test list; QW modifier required for Medicare (Pub 100-04, ch. 16)."
_BASIS_TERM = "X12 271 plan-date semantics; coverage must be verified as-of the date of service."
_BASIS_MOLDX = "Palmetto GBA MolDX — DEX Z-Code registration required for molecular tests (MolDX LCDs)."


@dataclass
class Finding:
    code: str                       # machine code, e.g. "NCCI_PTP"
    severity: str                   # info | advisory | warn | block
    message: str                    # plain-English explanation
    basis: str                      # the PUBLIC standard this rests on
    action: str = ""                # recommended next step
    cpt: str = ""                   # primary code the finding is about
    related: list[str] = field(default_factory=list)  # other codes involved
    method: str = METHOD

    def to_dict(self) -> dict:
        return {
            "code": self.code, "severity": self.severity, "message": self.message,
            "basis": self.basis, "action": self.action, "cpt": self.cpt,
            "related": self.related, "method": self.method,
        }


def _as_of_dos_termed(coverage: CoverageResult, dos: str) -> Optional[Finding]:
    """Retroactive / as-of-DOS termination risk — the miss incumbents make most."""
    if coverage.status == CoverageStatus.TERMED:
        return Finding(
            "COVERAGE_TERMED", SEV_WARN,
            "Coverage is termed — do not run on assumed coverage.",
            _BASIS_TERM, "Re-verify eligibility as of the date of service before releasing.",
        )
    try:
        d0 = date.fromisoformat(dos)
    except (ValueError, TypeError):
        return None
    if coverage.term_date:
        try:
            td = date.fromisoformat(coverage.term_date)
            if td < d0:
                return Finding(
                    "TERM_BEFORE_DOS", SEV_WARN,
                    f"Plan term date {coverage.term_date} precedes the date of service.",
                    _BASIS_TERM, "Re-verify as of DOS; likely no active coverage.")
            if (td - d0).days <= 30:
                return Finding(
                    "TERM_NEAR_DOS", SEV_ADVISORY,
                    f"Plan term date {coverage.term_date} is within 30 days of service.",
                    _BASIS_TERM, "Re-verify close to DOS to catch a retroactive term.")
        except ValueError:
            pass
    if coverage.effective_date:
        try:
            ed = date.fromisoformat(coverage.effective_date)
            if ed > d0:
                return Finding(
                    "NOT_YET_EFFECTIVE", SEV_WARN,
                    f"Plan effective date {coverage.effective_date} is after the date of service.",
                    _BASIS_TERM, "Confirm the correct plan/coverage period for this DOS.")
        except ValueError:
            pass
    return None


def run_intercept(req: PatientRequest, coverage: CoverageResult, lines) -> list[dict]:
    """Run every claim-integrity rule over one accession; return ordered findings.

    `lines` is the list of per-CPT dispositions from the gate (each has .cpt and
    .allowed_amount). Pure function: no side effects, deterministic.
    """
    findings: list[Finding] = []
    ordered = [(getattr(ln, "cpt", "") or "").strip() for ln in lines]
    ordered = [c for c in ordered if c]
    present = set(ordered)
    payer = coverage.payer_name or req.payer_name or ""
    medicare = is_traditional_medicare(payer)

    # 1) NCCI PTP bundling — a covered add-on that will not be separately paid.
    for col1, components in _NCCI_PTP.items():
        if col1 in present:
            for col2 in sorted(components & present):
                findings.append(Finding(
                    "NCCI_PTP", SEV_WARN,
                    f"{col2} is bundled into comprehensive panel {col1} and is not "
                    f"separately payable unless a distinct-service modifier "
                    f"({'/'.join(_PTP_BYPASS_MODIFIERS[:2])}) is documented.",
                    _BASIS_NCCI,
                    f"Drop {col2}, or append a distinct-service modifier only if clinically justified.",
                    cpt=col2, related=[col1]))

    # 2) MUE / duplicate units — same code ordered beyond its per-DOS cap.
    for cpt in sorted(present):
        cap = _MUE_MAX_UNITS.get(cpt)
        count = ordered.count(cpt)
        if cap is not None and count > cap:
            findings.append(Finding(
                "MUE_EXCEEDED", SEV_WARN,
                f"{cpt} ordered {count}x on one date of service exceeds the MUE cap of {cap}.",
                _BASIS_MUE,
                "Confirm the extra unit(s) are warranted and documented, or remove the duplicate.",
                cpt=cpt))

    # 3) CLIA-waived tests billed to Medicare require the QW modifier.
    if medicare:
        for cpt in sorted(present & _CLIA_WAIVED_QW):
            findings.append(Finding(
                "QW_MODIFIER", SEV_ADVISORY,
                f"{cpt} is CLIA-waived; Medicare requires the QW modifier to pay it.",
                _BASIS_QW, f"Append modifier QW to {cpt} for Medicare claims.",
                cpt=cpt))

    # 4) MolDX Z-code registration for molecular tests under Medicare/MolDX.
    for cpt in sorted(present):
        pol = get_policy(cpt)
        if pol and pol.moldx_zcode and medicare:
            findings.append(Finding(
                "MOLDX_ZCODE", SEV_WARN,
                f"{cpt} is a molecular test requiring MolDX DEX Z-Code registration to be paid.",
                _BASIS_MOLDX, f"Attach the registered DEX Z-Code for {cpt} before claim submission.",
                cpt=cpt))

    # 5) As-of-DOS termination / effective-date risk (accession-level).
    term = _as_of_dos_termed(coverage, req.dos)
    if term:
        findings.append(term)

    findings.sort(key=lambda f: _SEV_RANK.get(f.severity, 0), reverse=True)
    return [f.to_dict() for f in findings]


def summarize_findings(findings: list[dict]) -> dict:
    """Count findings by severity for a compact banner."""
    out = {SEV_BLOCK: 0, SEV_WARN: 0, SEV_ADVISORY: 0, SEV_INFO: 0}
    for f in findings or []:
        sev = f.get("severity", SEV_INFO)
        out[sev] = out.get(sev, 0) + 1
    out["total"] = sum(v for k, v in out.items() if k != "total")
    return out
