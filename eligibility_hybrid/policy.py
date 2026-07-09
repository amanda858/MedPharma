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
    # ── Pharmacogenomics (PGx): drug-metabolism gene analysis ──
    # Medicare covers PGx only to guide a specific drug with a PGx-informed FDA
    # label / CPIC guideline; multi-gene panels and MTHFR are routinely
    # non-covered (→ ABN required to bill the patient).
    "81225": CptPolicy(
        "81225", "CYP2C19 (drug metabolism) gene analysis (PGx)",
        ["Z79", "Z51.81", "F32", "F33", "F41", "I25", "I63"],
        moldx_zcode="REQUIRED", cost_to_run=150.0,
        notes="PGx single-gene: covered only when guiding a specific drug; else ABN."),
    "81226": CptPolicy(
        "81226", "CYP2D6 (drug metabolism) gene analysis (PGx)",
        ["Z79", "Z51.81", "F32", "F33", "F41", "G89", "M79"],
        moldx_zcode="REQUIRED", cost_to_run=150.0,
        notes="PGx single-gene: necessity tied to a specific drug indication; else ABN."),
    "81227": CptPolicy(
        "81227", "CYP2C9 (drug metabolism) gene analysis (PGx)",
        ["Z79.01", "Z79", "I48", "Z51.81"],
        moldx_zcode="REQUIRED", cost_to_run=150.0,
        notes="PGx single-gene (e.g., warfarin): tie to the drug or issue ABN."),
    "81291": CptPolicy(
        "81291", "MTHFR gene analysis (common variants)",
        [], moldx_zcode="REQUIRED", cost_to_run=120.0,
        notes="Medicare NON-COVERED as not reasonable & necessary — ABN required to bill patient."),
    "81418": CptPolicy(
        "81418", "Drug metabolism (PGx) genomic sequencing panel",
        [], prior_auth_required=True, moldx_zcode="REQUIRED", cost_to_run=350.0,
        notes="PGx PANEL: routinely non-covered by Medicare — ABN required; commercial needs PA."),
    # ── Next-Generation Sequencing (NGS): tumor / molecular profiling ──
    # NCD 90.2: somatic NGS is covered for advanced cancer (Stage III/IV,
    # recurrent, relapsed, refractory, or metastatic) with an FDA-approved
    # companion diagnostic; MolDX DEX Z-code registration required.
    "81445": CptPolicy(
        "81445", "Solid tumor NGS panel, 5-50 genes",
        ["C", "D0", "D3", "D4"],
        prior_auth_required=True, moldx_zcode="REQUIRED", cost_to_run=600.0,
        notes="NCD 90.2: advanced-cancer Dx + FDA companion dx; MolDX Z-code required."),
    "81449": CptPolicy(
        "81449", "Solid tumor NGS panel (RNA), 5-50 genes",
        ["C", "D0", "D3", "D4"],
        prior_auth_required=True, moldx_zcode="REQUIRED", cost_to_run=650.0,
        notes="NCD 90.2 advanced-cancer criteria; MolDX Z-code required."),
    "81455": CptPolicy(
        "81455", "Solid tumor NGS panel, 51+ genes",
        ["C", "D0", "D3", "D4"],
        prior_auth_required=True, moldx_zcode="REQUIRED", cost_to_run=900.0,
        notes="NCD 90.2 advanced-cancer criteria; large panel — MolDX Z-code required."),
    "81457": CptPolicy(
        "81457", "Solid organ neoplasm genomic sequencing, DNA, 5-50 genes",
        ["C", "D0", "D3", "D4"],
        prior_auth_required=True, moldx_zcode="REQUIRED", cost_to_run=700.0,
        notes="MolDX genomic profiling; advanced-cancer medical necessity."),
}

_MEDICARE_HINTS = ("medicare", "cms", "part b")
# Common data-entry misspellings of "medicare" seen in real payer feeds.
_MEDICARE_MISSPELLINGS = ("medicre", "medcare", "medicar", "mdicare", "medciare", "medacare")

# Commercial carrier brands. Original / Traditional Medicare is administered by
# CMS and is NEVER carrier-branded, so a carrier brand alongside any Medicare or
# managed-plan signal identifies a Medicare Advantage (Part C) product.
_MA_CARRIER_BRANDS = (
    "humana", "aetna", "cigna", "united", "uhc", "unitedhealth", "optum",
    "wellcare", "wellmed", "anthem", "bcbs", "blue cross", "blue shield",
    "kaiser", "molina", "centene", "elevance", "devoted", "clover", "oscar",
    "scan health", "healthspring", "coventry", "amerigroup", "alignment",
    "brand new day", "essence", "peoples health",
)
# Managed-plan type markers — meaningful when Medicare is also indicated, since
# Original Medicare is straight fee-for-service (never PPO/HMO/PFFS/SNP).
_PLAN_TYPE_MARKERS = ("ppo", "hmo", "pffs", "snp", "part c", "advantage")
# Product markers that signal Medicare Advantage even without the word "medicare"
# (e.g. "Humana Gold Plus", "UHC Medicare Complete", "… Choice/Solutions").
_MA_PRODUCT_MARKERS = (
    "advantage", "part c", "gold", "choice", "complete", "solutions",
    "medicareblue", "medicare blue", "gold plus",
)
# Known Medicare Advantage product names that may omit "medicare" entirely.
_MA_PRODUCT_NAMES = ("wellcare", "humana gold", "medicare complete")


def _mentions_medicare(p: str) -> bool:
    """True if the (lowercased) payer text references Medicare, tolerating the
    common misspellings found in uploaded payer data."""
    return (any(h in p for h in _MEDICARE_HINTS)
            or any(m in p for m in _MEDICARE_MISSPELLINGS))


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


def is_medicare_advantage(payer_name: str) -> bool:
    """True when the payer is a Medicare Advantage (Part C) plan rather than
    Original / Traditional Medicare.

    Real-world MA names are branded by the managed-care carrier (Humana, Aetna,
    UHC, …) and/or carry a plan-type marker (PPO, HMO, Complete, Gold, Choice,
    Solutions, …); they seldom contain the literal words "Medicare Advantage".
    Misclassifying MA as Original Medicare routes the 270 to CMS HETS — which
    only answers for fee-for-service Medicare — and applies the wrong financial-
    liability rules (CMS ABN instead of a plan organization determination)."""
    p = (payer_name or "").lower().strip()
    if not p:
        return False
    brand = any(b in p for b in _MA_CARRIER_BRANDS)
    # Branded or managed "Medicare …" product → Advantage (Original Medicare is
    # never branded and is straight FFS, so it never trips these signals).
    if _mentions_medicare(p) and (brand or any(m in p for m in _PLAN_TYPE_MARKERS)):
        return True
    # Carrier brand + an MA product marker, even without the word "medicare".
    if brand and any(m in p for m in _MA_PRODUCT_MARKERS):
        return True
    # Known MA-only product names.
    if any(n in p for n in _MA_PRODUCT_NAMES):
        return True
    # Explicit Advantage / Part C wording.
    return "advantage" in p or "part c" in p


def is_traditional_medicare(payer_name: str) -> bool:
    """True only for Original / Traditional Medicare (fee-for-service Part A/B) —
    the payer CMS HETS answers for. Carrier-branded Medicare products (Medicare
    Advantage / Part C) and non-Medicare payers return False. Railroad Medicare
    remains Original Medicare (FFS), though it is administered by the RRB
    Specialty MAC (Palmetto GBA) and needs that payer ID when routing a 270."""
    p = (payer_name or "").lower()
    if not p or is_medicare_advantage(payer_name):
        return False
    return _mentions_medicare(p)


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


# ── Pharmacogenomics / NGS classification + ABN (Advance Beneficiary Notice) ──
# The high-financial-risk molecular tests where Medicare medical necessity is
# strict and a signed ABN (CMS-R-131) is frequently required before billing.
_PGX_CPTS = {
    "81225", "81226", "81227", "81230", "81231", "81232", "81291", "81328",
    "81335", "81346", "81355", "81418", "81419",
}
_NGS_CPTS = {
    "81445", "81449", "81450", "81455", "81456", "81457", "81458", "81459",
    "81462", "81463", "81464", "81479",
}
# Generally NOT reasonable & necessary under Medicare → ABN almost always required.
_MEDICARE_NONCOVERED_MOLECULAR = {"81291", "81418", "81419"}


def is_pgx(cpt: str) -> bool:
    """Pharmacogenomics (drug-metabolism) gene test."""
    return (cpt or "").strip() in _PGX_CPTS


def is_ngs(cpt: str) -> bool:
    """Next-generation sequencing / large molecular panel."""
    return (cpt or "").strip() in _NGS_CPTS


def is_high_risk_molecular(cpt: str) -> bool:
    """NGS, PGx, or unlisted molecular — Medicare necessity is strict and an ABN
    is frequently required before the specimen is collected."""
    c = (cpt or "").strip()
    return c in _PGX_CPTS or c in _NGS_CPTS or c == "81479"


@dataclass
class AbnGuidance:
    """Advance Beneficiary Notice (CMS-R-131) determination for one test."""
    abn_required: bool           # signed ABN needed before collection to bill patient
    applies: bool                # ABN mechanism applies (Original Medicare Part B)
    modifier: str                # claim modifier: GA / GZ / GX / GY / ""
    reason: str                  # plain-English why
    entails: list[str] = field(default_factory=list)   # what the ABN entails (steps)


def _abn_entails(cpt: str, desc: str) -> list[str]:
    return [
        "Deliver the ABN (form CMS-R-131) to the patient BEFORE specimen collection.",
        f"State the test ({desc}, CPT {cpt}), the reason Medicare may deny "
        "('not reasonable and necessary'), and a good-faith cost estimate.",
        "Patient selects Option 1 (proceed, bill Medicare, accept financial "
        "responsibility if denied) and signs & dates the notice.",
        "Keep the signed ABN on file and append modifier GA to the claim line.",
        "No signed ABN? You must use GZ — Medicare will deny and the lab writes "
        "off the charge; the patient CANNOT be billed.",
    ]


def abn_recommendation(cpt: str, payer_name: str, necessary: bool,
                       frequency_ok: bool = True, plan_type: str = "") -> AbnGuidance:
    """Decide whether an ABN is required to bill a test, and what that entails.

    ABN (CMS-R-131) applies to Original / Traditional Medicare Part B only — it
    shifts financial liability to the patient when Medicare is expected to deny a
    service as not reasonable & necessary. Medicare Advantage instead needs a
    pre-service organization determination; commercial plans use their own
    financial-responsibility waiver.
    """
    cpt = (cpt or "").strip()
    pol = get_policy(cpt)
    desc = pol.description if pol else f"CPT {cpt}"
    trad_medicare = is_traditional_medicare(payer_name)
    advantage = is_medicare_advantage(payer_name) or \
        (plan_type or "").strip().lower() in ("medicare advantage", "advantage", "ma")

    # Medicare Advantage — the CMS ABN does not apply.
    if advantage:
        return AbnGuidance(
            False, False, "",
            "Medicare Advantage — the CMS-R-131 ABN does not apply. Obtain a "
            "pre-service organization determination / prior authorization from the "
            "plan; use the plan's advance written notice to establish member liability.",
            ["Request a pre-service organization determination / PA from the plan.",
             "If the plan denies, give the member the plan's advance written notice "
             "before service to transfer financial liability."])

    # Non-Medicare payer — commercial waiver, not a CMS ABN.
    if not trad_medicare:
        return AbnGuidance(
            False, False, "",
            "Non-Medicare payer — the CMS ABN does not apply. If non-coverage is "
            "expected, use your commercial financial-responsibility / waiver form and "
            "confirm prior authorization first.",
            ["Confirm prior authorization / medical policy with the commercial payer.",
             "If non-coverage is expected, have the patient sign a commercial "
             "financial-responsibility waiver before service."])

    # ── Original / Traditional Medicare ──
    if cpt in _MEDICARE_NONCOVERED_MOLECULAR:
        return AbnGuidance(
            True, True, "GA",
            f"{desc} (CPT {cpt}) is generally non-covered by Medicare as not "
            "reasonable & necessary. A signed ABN is required to bill the patient "
            "(modifier GA); without it the charge is a write-off (GZ).",
            _abn_entails(cpt, desc))

    if not necessary:
        return AbnGuidance(
            True, True, "GA",
            f"The documented diagnosis does not meet Medicare medical-necessity "
            f"(LCD/NCD) criteria for {desc} (CPT {cpt}). Expect a denial — obtain a "
            "signed ABN and append GA, or the patient cannot be billed (GZ).",
            _abn_entails(cpt, desc))

    if not frequency_ok:
        return AbnGuidance(
            True, True, "GA",
            f"{desc} (CPT {cpt}) exceeds Medicare frequency limits for this patient — "
            "expect a frequency denial. Obtain a signed ABN (GA) before collection.",
            _abn_entails(cpt, desc))

    if is_high_risk_molecular(cpt):
        return AbnGuidance(
            False, True, "",
            f"{desc} (CPT {cpt}) meets medical necessity on the documented diagnosis, "
            "but NGS/PGx are audit-sensitive under MolDX — keep clinical documentation "
            "on file and register the DEX Z-code. If necessity is borderline, deliver a "
            "protective ABN (GA) before collection.",
            ["Confirm the qualifying Dx and ordering-provider documentation is on file.",
             "Register the MolDX DEX Z-code before billing.",
             "If necessity is borderline, deliver a protective ABN (GA) before collection."])

    return AbnGuidance(
        False, True, "",
        f"{desc} (CPT {cpt}) meets Medicare medical necessity — no ABN needed.",
        [])
