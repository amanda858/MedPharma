"""Outreach playbook — personalized hooks, objection handlers, heat scoring.

Designed for one-click cold-DM workflow. Every lead gets:

  • A *personalized hook line* tuned to their lab type + state +
    NPPES update recency (so every DM feels custom-written).
  • A library of *objection-handler replies* the user can paste when
    a prospect pushes back ("we already have a biller", "send info",
    "what does it cost", "not interested", "busy now").
  • A *heat score* combining lab type, state RCM-pain index, NPPES
    recency, and contact richness — used to sort the daily Top 10.
  • A *Calendly link* injected from CALENDLY_URL env var so booking
    is one tap from the DM.

Zero external APIs. Pure rule-based, deterministic, free.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional


# ─── Calendly link (single env var, set once) ─────────────────────────

def calendly_link() -> str:
    """Configurable booking link injected into all templates."""
    return (os.getenv("CALENDLY_URL") or "https://calendly.com/medpharmasc/15min").strip()


# ─── Lab-type pain points (specific, dollar-figure, credible) ─────────

_LAB_PAIN_POINTS: dict[str, str] = {
    # Toxicology — UDT denials are the #1 industry pain
    "toxicology": (
        "UDT denials from Medicare LCD changes — most tox labs lose "
        "12–18% of net rev to G0480/G0483 documentation gaps"
    ),
    "clinical": (
        "denial rates climbing on 80050/80053 panels — most clinical "
        "labs leak 8–14% of net rev to coding & medical-necessity edits"
    ),
    "pathology": (
        "TC/PC split billing errors and 88305 medical-necessity denials "
        "— pathology groups typically lose 9–15% of net rev here"
    ),
    "molecular": (
        "MolDx / Z-code prior-auth denials — molecular labs are seeing "
        "the highest denial growth of any specialty (often 15–22% of net rev)"
    ),
    "genetic": (
        "Z-code registration gaps and prior-auth denials — most genetic "
        "labs lose 14–20% of net rev to MolDx coverage policy edits"
    ),
    "cytopathology": (
        "88142/88175 bundling and screening-vs-diagnostic Pap denials — "
        "cyto labs leak 7–12% of net rev to these"
    ),
    "blood bank": (
        "transfusion-medicine LCD edits and 86850-series denials — "
        "blood-bank ops lose 6–10% of net rev to these gaps"
    ),
    "histocompatibility": (
        "transplant-related coverage and 86825-series denials — "
        "HLA labs leak 8–12% of net rev to coverage policy issues"
    ),
    "physician office": (
        "POL CLIA-waived billing errors and place-of-service denials "
        "— most office labs lose 6–10% of net rev to these"
    ),
    "diagnostic": (
        "imaging + lab cross-billing denials and credentialing gaps — "
        "most diagnostic centers leak 10–15% of net rev"
    ),
    "default": (
        "denial-rate creep and slow A/R — most labs we look at are "
        "leaking 10–18% of net revenue to fixable RCM gaps"
    ),
}


def _lab_pain_for(taxonomy_desc: str = "", lab_type_detected: str = "") -> str:
    """Pick the most specific pain point for this lab."""
    blob = f"{taxonomy_desc} {lab_type_detected}".lower()
    for key, pain in _LAB_PAIN_POINTS.items():
        if key in blob:
            return pain
    return _LAB_PAIN_POINTS["default"]


# ─── State-level RCM signal (high-pain states first) ──────────────────

_HIGH_PAIN_STATES = {
    "FL", "TX", "CA", "NY", "PA", "GA", "NC", "OH", "TN", "AZ",
    "NJ", "NV", "MI", "IL", "VA", "SC", "AL",
}


def _state_signal(state: str) -> str:
    s = (state or "").upper()
    if s in _HIGH_PAIN_STATES:
        return f"{s} labs especially — payer-mix shifts there are crushing collections right now"
    return ""


# ─── NPPES recency signal ──────────────────────────────────────────────

def _recency_signal(last_updated: str) -> tuple[str, int]:
    """Returns (label, days_since_update). Recently updated = warmer prospect."""
    if not last_updated:
        return ("", 9999)
    dt = None
    s = last_updated.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(s[:10], fmt)
                break
            except Exception:
                continue
    if dt is None:
        return ("", 9999)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    days = (datetime.now(timezone.utc) - dt).days
    if days <= 90:
        return ("HOT — NPI updated <90d ago (org in flux)", days)
    if days <= 365:
        return ("WARM — NPI updated this year", days)
    return ("", days)


# ─── Personalized hook generator ──────────────────────────────────────

def personalized_hook(
    first: str,
    org: str,
    taxonomy_desc: str = "",
    lab_type_detected: str = "",
    state: str = "",
    last_updated: str = "",
) -> str:
    """One tailored opening line per lead. Drops into LinkedIn/FB/SMS."""
    first = (first or "").split()[0].strip().title() or "there"
    pain = _lab_pain_for(taxonomy_desc, lab_type_detected)
    state_bit = _state_signal(state)
    recency, _ = _recency_signal(last_updated)

    hook = f"Hi {first} — quick one specific to {org or 'your lab'}: {pain}."
    if state_bit:
        hook += f" {state_bit}."
    if recency.startswith("HOT"):
        hook += " (And I noticed your NPI was updated recently — usually means the org is in transition, which is the best time to fix this stuff.)"
    return hook


# ─── Objection handlers (paste-ready replies) ─────────────────────────

def objection_handlers(
    first: str = "",
    org: str = "",
    sender_name: str = "Eric",
    sender_company: str = "MedPharma SC",
) -> dict:
    """Pre-written replies for the 5 most common pushbacks."""
    first = (first or "").split()[0].strip().title() or "there"
    org = org.strip() or "your lab"
    book = calendly_link()
    sender = sender_name.strip() or "Eric"

    return {
        "objection_already_have_biller": (
            f"Totally fair — most labs we work with already had a biller "
            f"when they came to us. We don't replace, we audit. "
            f"Free 15-min review of your last 90 days of denials & A/R; "
            f"if your current biller is solid, I'll tell you straight up. "
            f"If they're missing money, I'll show you where. {book}"
        ),
        "objection_send_info_first": (
            f"Happy to. Quickest way is for me to pull a 1-pager tailored "
            f"to {org} — takes 2 min once I know your top 2 payers and "
            f"rough monthly claim volume. Reply with those and I'll send "
            f"it tonight. Or grab any slot here: {book}"
        ),
        "objection_what_does_it_cost": (
            f"Depends on your volume + scope, but ballpark: most lab "
            f"engagements run on a % of collections (no recovery, no fee), "
            f"or a flat retainer if you prefer predictability. Either way, "
            f"the 15-min review is free and you walk away with concrete "
            f"numbers either way. {book}"
        ),
        "objection_not_interested": (
            f"No problem {first} — I'll get out of your inbox. If RCM ever "
            f"jumps up the priority list, I'm easy to find. Wishing {org} "
            f"the best. — {sender}"
        ),
        "objection_busy_now": (
            f"Understood — labs are slammed right now. I'll circle back in "
            f"~30 days. If sooner makes sense, here's my calendar: {book}. "
            f"Either way, no pressure. — {sender}"
        ),
        "objection_who_are_you": (
            f"Fair question. {sender_company} — we handle billing, "
            f"credentialing, and denial recovery for independent diagnostic "
            f"labs (toxicology, molecular, clinical, pathology). Happy to "
            f"share client refs from labs your size. {book}"
        ),
    }


# ─── Heat score (drives the daily Top 10) ─────────────────────────────

def heat_score(
    lead_score: int = 0,
    fit_score: int = 0,
    has_dm: bool = False,
    has_direct_line: bool = False,
    has_verified_domain: bool = False,
    has_social: bool = False,
    last_updated: str = "",
    state: str = "",
) -> tuple[int, list[str]]:
    """Combine signals into a 0-100 buyability/heat score + reasons."""
    score = 0
    reasons: list[str] = []

    # Base lab fit
    score += min(40, int(lead_score or 0) * 0.4)
    score += min(20, int(fit_score or 0) * 0.2)

    if has_dm:
        score += 10
        reasons.append("named DM")
    if has_direct_line:
        score += 8
        reasons.append("direct phone")
    if has_verified_domain:
        score += 6
        reasons.append("verified domain")
    if has_social:
        score += 6
        reasons.append("social DM ready")

    label, days = _recency_signal(last_updated)
    if label.startswith("HOT"):
        score += 15
        reasons.append("NPI <90d update (in flux)")
    elif label.startswith("WARM"):
        score += 6
        reasons.append("NPI updated this year")

    if (state or "").upper() in _HIGH_PAIN_STATES:
        score += 5
        reasons.append("high-pain state")

    return min(100, int(score)), reasons


# ─── Inject Calendly + hook into DM templates ─────────────────────────

def enrich_templates_with_hook(
    templates: dict,
    hook: str,
    sender_name: str = "Eric",
) -> dict:
    """Replace generic openers with the personalized hook + add Calendly CTA."""
    if not templates:
        return templates
    book = calendly_link()
    sender = sender_name.strip() or "Eric"
    out = dict(templates)

    # LinkedIn first message — replace bland intro with hook + CTA
    if hook and out.get("linkedin_first_message"):
        out["linkedin_first_message"] = (
            f"{hook}\n\n"
            f"I run {sender}'s shop — we fix exactly this kind of leak for "
            f"independent labs without replacing your current biller. "
            f"15-min review is free; if there's nothing to fix I'll tell "
            f"you straight.\n\n"
            f"Grab any slot: {book}\n\n— {sender}"
        )

    if hook and out.get("facebook_dm"):
        out["facebook_dm"] = f"{hook} Free 15-min RCM review: {book} — {sender}"

    if hook and out.get("instagram_dm"):
        out["instagram_dm"] = f"{hook[:120]}… Free 15-min review: {book} — {sender}"

    if hook and out.get("x_dm"):
        msg = f"{hook} Free 15-min review: {book} — {sender}"
        if len(msg) > 275:
            msg = msg[:272] + "..."
        out["x_dm"] = msg

    if out.get("sms"):
        sms = (out["sms"].rstrip(".") + f". {book}").strip()
        if len(sms) > 160:
            sms = sms[:157] + "..."
        out["sms"] = sms

    return out
