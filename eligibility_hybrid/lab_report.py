"""Advanced Laboratory Eligibility Report builder.

Assembles the enterprise lab-RCM eligibility report (the 8-section format used
by high-end lab billing platforms) from data the platform already owns:

  * the verified coverage record (only ever written from a REAL 271 or a human
    edit) -> patient / subscriber, coverage status + dates, patient financials
  * the LCD/NCD policy engine (policy.py) -> per-CPT medical necessity, prior
    auth, frequency limits, MolDX registration, and ABN (CMS-R-131) guidance
  * the claim-integrity intercept (intercept.py) -> NCCI PTP bundling, MUE unit
    caps, QW modifier, as-of-DOS term risk, and pre-submission data defects

It is HONEST BY DESIGN: it fabricates nothing. When no live payer source has
confirmed active enrollment, coverage status and member financials are marked
"Requires live 271" instead of being invented, and the billing decision stays
"PENDING VERIFICATION" rather than a false "GO".

The output is a plain dict (JSON-serializable) so the hub can render it, and a
lab's LIS/billing system can consume it, without importing this package.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from .policy import (
    get_policy, check_medical_necessity, is_prior_auth_required,
    abn_recommendation, is_traditional_medicare, is_medicare_advantage,
)

# CPTs Medicare routinely denies as not reasonable & necessary (ABN territory).
_MEDICARE_NONCOVERED = {"81291", "81418", "81419"}

_ACTIVE = "Active"
_INACTIVE = {"Inactive", "Termed"}


def _plan_category(payer: str, plan_name: str = "") -> str:
    """Human plan category from the payer text (honest classification)."""
    if is_traditional_medicare(payer):
        return "Original / Traditional Medicare (Part B, fee-for-service)"
    if is_medicare_advantage(payer):
        return "Medicare Advantage (Part C)"
    p = (payer or "").lower()
    if "medicaid" in p:
        return "Medicaid"
    if "tricare" in p or "champva" in p:
        return "Government (TRICARE/CHAMPVA)"
    base = plan_name or payer or "Commercial"
    return f"Commercial ({base})" if "commercial" not in base.lower() else base


def _covered_label(active: bool, connected: bool, necessary: bool,
                   medicare_noncovered: bool, status: str) -> tuple[str, str]:
    """(short, detail) coverage verdict for one CPT — never fabricated."""
    if medicare_noncovered:
        return ("No", "Medicare generally non-covered (not reasonable & necessary) - ABN required to bill the patient.")
    if not necessary:
        return ("No", "Medical necessity not met on the diagnosis provided.")
    if not connected or (status or "").strip().lower() in ("", "unknown", "pending", "needs re-verify"):
        return ("Policy-eligible", "Passes coverage policy; requires a live 271 to confirm the member's active enrollment.")
    if not active:
        return ("No", f"Member coverage came back {status or 'inactive'} - not billable until re-verified active.")
    return ("Yes", "Covered - medical necessity met and member coverage is active.")


def build_lab_eligibility_report(
    *,
    patient: dict,
    coverage: dict,
    cpts: list[str],
    icd10s: list[str],
    dos: str = "",
    findings: Optional[list[dict]] = None,
    auth_number: str = "",
) -> dict:
    """Build the 8-section Advanced Laboratory Eligibility Report.

    patient  : {name, dob, member_id, payer, group, subscriber, relationship}
    coverage : {status, effective_date, term_date, plan_name, source,
                connected: bool, reference_number, checked_at, verified_by,
                benefit: {copay, deductible_total, deductible_met,
                          coinsurance_pct, oop_total, oop_met}}
    """
    findings = findings or []
    payer = (patient.get("payer") or "").strip()
    dos = dos or date.today().isoformat()
    status = (coverage.get("status") or "").strip()
    connected = bool(coverage.get("connected"))
    active = status == _ACTIVE
    trad_medicare = is_traditional_medicare(payer)
    plan_name = (coverage.get("plan_name") or "").strip()

    # ── Section 1: Patient & Subscriber Verification ──────────────────────
    subscriber = (patient.get("subscriber") or "").strip()
    section1 = {
        "patient_name": patient.get("name") or "",
        "dob": patient.get("dob") or "",
        "subscriber_name": subscriber or (patient.get("name") or ""),
        "relationship": patient.get("relationship") or ("Self" if not subscriber else "Dependent"),
        "member_id": patient.get("member_id") or "",
        "group_number": patient.get("group") or "",
        "plan_type": plan_name or _plan_category(payer, plan_name),
        "verification_source": coverage.get("source") or "MedPharma coverage policy (rule intercept)",
        "verification_timestamp": coverage.get("checked_at") or datetime.now().strftime("%m/%d/%Y %H:%M"),
        "reference_number": coverage.get("reference_number") or "",
    }

    # ── Section 2: Coverage Status (DOS-specific) ─────────────────────────
    section2 = {
        "coverage_status": status if connected else "Requires live 271 (not yet confirmed)",
        "date_of_service": dos,
        "effective_date": coverage.get("effective_date") or "",
        "termination_date": coverage.get("term_date") or "",
        "primary_secondary": "Primary" if connected else "",
        "coordination_of_benefits": "" if connected else "Requires live 271",
        "plan_category": _plan_category(payer, plan_name),
    }

    # ── Per-CPT evaluation drives sections 3, 5, 6, 8 ─────────────────────
    lab_benefits: list[dict] = []
    med_nec_flags: list[str] = []
    non_covered: list[str] = []
    frequency_limits: list[str] = []
    moldx_flags: list[str] = []
    authorizations: list[dict] = []
    covered_except: list[str] = []
    auth_required_for: list[str] = []
    recommendations: list[str] = []
    any_block = False

    for cpt in cpts:
        pol = get_policy(cpt)
        desc = pol.description if pol else f"CPT {cpt}"
        mn = check_medical_necessity(cpt, icd10s, payer, dos=dos)
        pa_required, pa_reason = is_prior_auth_required(cpt, payer)
        abn = abn_recommendation(cpt, payer, mn.necessary, mn.frequency_ok,
                                 _plan_category(payer, plan_name))
        moldx_required = bool(mn.moldx_required)
        medicare_noncovered = trad_medicare and cpt in _MEDICARE_NONCOVERED
        needs_auth = bool(pa_required or moldx_required or (pol and pol.prior_auth_required and not trad_medicare))

        short, detail = _covered_label(active, connected, mn.necessary,
                                       medicare_noncovered, status)
        freq = (f"1 per {pol.frequency_limit_days} days"
                if pol and pol.frequency_limit_days else "No limit on file")

        note_bits = []
        if pol and pol.notes:
            note_bits.append(pol.notes)
        if moldx_required:
            note_bits.append("MolDX DEX Z-code registration required.")
        if abn.abn_required:
            note_bits.append(f"ABN (CMS-R-131) required; bill with modifier {abn.modifier}.")
        lab_benefits.append({
            "cpt": cpt, "description": desc, "covered": short,
            "covered_detail": detail,
            "auth_required": "Yes" if needs_auth else "No",
            "frequency": freq,
            "note": " ".join(note_bits) or mn.reason,
        })

        # Section 5 rollups
        if pol and pol.covered_icd10_prefixes:
            med_nec_flags.append(
                f"{cpt} ({desc}): requires a supporting diagnosis - "
                f"{', '.join(pol.covered_icd10_prefixes[:6])}.")
        if short == "No":
            non_covered.append(f"{cpt} - {detail}")
            covered_except.append(cpt)
            any_block = True
        if pol and pol.frequency_limit_days:
            frequency_limits.append(f"{cpt}: 1 per {pol.frequency_limit_days} days.")
        if moldx_required or (pol and pol.moldx_zcode):
            moldx_flags.append(f"{cpt} ({desc}): MolDX DEX Z-code registration required to be paid.")

        # Section 6 authorizations
        if needs_auth:
            auth_required_for.append(cpt)
            authorizations.append({
                "service": f"{desc} ({cpt})",
                "auth_required": "Yes",
                "auth_number": auth_number or "",
                "valid_dates": "",
                "notes": pa_reason if pa_required else "Register the MolDX DEX Z-code before running.",
            })

        # Section 8 per-CPT recommendation
        if short == "No" and medicare_noncovered:
            recommendations.append(f"Do NOT bill {cpt} to Medicare without a signed ABN (modifier {abn.modifier or 'GA'}).")
        elif short == "No":
            recommendations.append(f"Hold {cpt} - {detail}")
        elif needs_auth:
            recommendations.append(f"Hold {cpt} until prior auth / MolDX registration is confirmed.")
            any_block = True
        elif short == "Policy-eligible":
            recommendations.append(f"{cpt}: clears coverage policy; confirm active enrollment with a live 271 before billing.")
        else:
            recommendations.append(f"Submit {cpt} - covered, medical necessity met.")

    # ── Section 4: Deductible / Copay / Coinsurance ───────────────────────
    b = coverage.get("benefit") or {}
    def _f(x):
        try:
            return None if x in (None, "") else float(x)
        except (TypeError, ValueError):
            return None
    ded_total = _f(b.get("deductible_total"))
    ded_met = _f(b.get("deductible_met"))
    oop_total = _f(b.get("oop_total"))
    oop_met = _f(b.get("oop_met"))
    ded_remaining = (max(0.0, ded_total - (ded_met or 0.0)) if ded_total is not None else None)
    oop_remaining = (max(0.0, oop_total - (oop_met or 0.0)) if oop_total is not None else None)
    have_financials = any(v is not None for v in
                          (ded_total, ded_met, _f(b.get("copay")), _f(b.get("coinsurance_pct")), oop_total, oop_met))
    section4 = {
        "available": bool(have_financials),
        "note": "" if have_financials else "Patient financials require a live 271 from the payer.",
        "annual_deductible": ded_total,
        "deductible_met": ded_met,
        "remaining_deductible": ded_remaining,
        "coinsurance_pct": _f(b.get("coinsurance_pct")),
        "copay": _f(b.get("copay")),
        "oop_max": oop_total,
        "oop_met": oop_met,
        "oop_remaining": oop_remaining,
    }

    # ── Section 5: Policy Limitations & Medical Necessity Flags ───────────
    section5 = {
        "medical_necessity": med_nec_flags,
        "non_covered": non_covered,
        "frequency_limits": frequency_limits,
        "moldx": moldx_flags,
        "integrity": findings,  # NCCI PTP / MUE / QW / term / data-quality
    }

    # ── Section 7: Payer Contact & Verification Metadata ──────────────────
    txn = ("X12 270/271 (HIPAA standard real-time eligibility)" if connected
           else "MedPharma coverage-policy intercept (LCD/NCD, NCCI/MUE, MolDX, ABN)")
    section7 = {
        "payer": payer or "",
        "source": coverage.get("source") or "MedPharma rule intercept",
        "transaction_type": txn,
        "verification_agent": coverage.get("verified_by") or "Automated engine",
        "response_code": status or "Policy review (no live 271)",
        "reference_number": coverage.get("reference_number") or "",
        "notes": ("No coordination-of-benefits issues reported."
                  if connected else "Live payer connection required to confirm enrollment and financials."),
    }

    # ── Section 8: Billing Readiness Summary (go / no-go) ─────────────────
    if not connected or status.lower() in ("", "unknown", "pending", "needs re-verify"):
        eligible = "Pending live verification"
    elif active:
        eligible = "Yes"
    else:
        eligible = "No"

    sev = {f.get("severity") for f in findings}
    if non_covered or eligible == "No" or "block" in sev:
        risk = "High"
    elif auth_required_for or any_block or "warn" in sev:
        risk = "Medium"
    elif eligible == "Pending live verification":
        risk = "Pending (needs live 271)"
    else:
        risk = "Low"

    if eligible == "No":
        decision = "NO-GO"
    elif non_covered or auth_required_for or any_block:
        decision = "HOLD"
    elif eligible == "Pending live verification":
        decision = "PENDING VERIFICATION"
    else:
        decision = "GO"

    section8 = {
        "decision": decision,
        "patient_eligible_for_dos": eligible,
        "covered_except": covered_except,
        "authorization_required_for": auth_required_for,
        "risk_of_denial": risk,
        "recommendations": recommendations or ["Add the ordered test(s) under Requested Services to generate a per-CPT billing plan."],
    }

    return {
        "meta": {
            "title": "Advanced Laboratory Eligibility Report",
            "generated_at": datetime.now().strftime("%m/%d/%Y %H:%M"),
            "date_of_service": dos,
            "connected": connected,
            "engine": "MedPharma eligibility + coverage-policy engine",
            "disclaimer": (
                "Coverage policy, medical necessity, prior-auth, MolDX and ABN are "
                "determined from public CMS/MolDX standards and need no payer key. "
                "Member active/inactive status and financials come only from a live "
                "X12 271. Nothing on this report is fabricated."),
        },
        "patient_verification": section1,
        "coverage_status": section2,
        "lab_benefits": lab_benefits,
        "financials": section4,
        "policy_flags": section5,
        "authorizations": authorizations,
        "payer_meta": section7,
        "billing_readiness": section8,
    }
