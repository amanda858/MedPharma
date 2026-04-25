"""Bulk lead prospector — pull fresh leads from NPPES, no CSV upload needed.

This is the "hunt mode" complement to the passive scrub-upload flow.
Given a state + optional specialty, query NPPES directly for every
matching lab org, then pipe them through the same scrubber/enrichment
pipeline so every prospect gets the full treatment:
  • Authorized-official name/title/phone
  • Verified domain
  • Social DM URLs (LinkedIn/FB/IG/X)
  • Personalized hook tuned to specialty + state
  • Heat score + Daily Top 10

High-intent filter: `new_only=True` returns only orgs enumerated in
the last 90 days — brand-new labs that haven't committed to a vendor
yet. These convert at far higher rates than incumbent prospects.

Data source: NPPES NPI Registry (free, no API key, official US registry
of healthcare providers). Same source we already use in production.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from app.config import NPI_API_BASE, NPI_API_VERSION, LAB_TAXONOMY_CODES


# Convenient specialty groups — each maps to one or more taxonomy keywords.
# Using the `taxonomy_description` query param against NPPES (free-text).
SPECIALTY_KEYWORDS: dict[str, list[str]] = {
    "clinical":        ["clinical medical laboratory"],
    "toxicology":      ["toxicology"],
    "pathology":       ["pathology"],
    "molecular":       ["molecular", "genetic"],
    "genetic":         ["genetic"],
    "blood_bank":      ["blood bank"],
    "cytopathology":   ["cytopathology", "cytology"],
    "histology":       ["histology"],
    "microbiology":    ["microbiology"],
    "physiological":   ["physiological laboratory"],
    "physician_office":["physician office"],
    "urgent_care":     ["urgent care"],
    "all_labs":        ["laboratory"],  # broadest net
}


# Org-name keywords that signal "this is actually a lab"
# (filters out NPPES orgs that are mis-registered under lab taxonomy —
# e.g. transportation companies, host committees, holding LLCs).
# We check the NAME only — taxonomy is already filtered by the query.
_LAB_NAME_KEYWORDS = (
    "lab", "laboratories", "laboratory", "diagnostic", "diagnostics",
    "pathology", "cytology", "histology", "molecular",
    "genetic", "genomic", "toxicology", "clinical", "blood",
    "microbiology", "immunology", "cytogenetics", "screening", "testing",
    "medical", "health", "hospital", "imaging", "urgent care",
    "phlebotomy", "dna", "biotech", "bio ", "serology", "pcr",
)

# Org-name keywords that signal "this is NOT a lab" (hard reject).
_NON_LAB_NAME_TOKENS = (
    "transportation", "transport ", "trucking", "host committee",
    "holdings", "holding group", "real estate", "properties", "realty",
    "capital", "ventures", "investments", "restaurant", "catering",
    "construction", "roofing", "auto ", "automotive",
)


def _looks_like_lab(name: str, taxonomy_desc: str = "") -> bool:
    """True if the org NAME contains lab-ish signal words AND no hard rejects."""
    n = (name or "").lower()
    if not n:
        return False
    for bad in _NON_LAB_NAME_TOKENS:
        if bad in n:
            return False
    return any(k in n for k in _LAB_NAME_KEYWORDS)


def _recent(enumeration_date: str, days: int = 90) -> bool:
    """True if this NPI was enumerated within the last N days."""
    if not enumeration_date:
        return False
    try:
        dt = datetime.fromisoformat(enumeration_date.replace("Z", "+00:00"))
    except Exception:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                dt = datetime.strptime(enumeration_date[:10], fmt)
                break
            except Exception:
                continue
        else:
            return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).days <= days


async def _query_npi_page(
    client: httpx.AsyncClient,
    *, state: str, taxonomy_kw: str, skip: int, limit: int = 200,
) -> list[dict]:
    params = {
        "version": NPI_API_VERSION,
        "limit":   min(limit, 200),
        "skip":    skip,
        "enumeration_type": "NPI-2",
        "state":   state.upper(),
        "taxonomy_description": taxonomy_kw,
    }
    try:
        r = await client.get(NPI_API_BASE, params=params, timeout=20.0)
        r.raise_for_status()
        return (r.json() or {}).get("results") or []
    except Exception:
        return []


async def prospect_state(
    state: str,
    specialty: str = "all_labs",
    limit: int = 100,
    new_only: bool = False,
    new_days: int = 90,
) -> list[dict]:
    """Pull fresh lab prospects from NPPES for one state.

    Returns a list of raw row-dicts in the same shape our scrubber
    accepts as input (org_name / city / state / etc.) so they flow
    straight into the existing pipeline.
    """
    kws = SPECIALTY_KEYWORDS.get(specialty) or [specialty]
    seen_npis: set[str] = set()
    out: list[dict] = []

    async with httpx.AsyncClient(timeout=25.0) as client:
        for kw in kws:
            skip = 0
            while len(out) < limit:
                page = await _query_npi_page(
                    client, state=state, taxonomy_kw=kw, skip=skip, limit=200,
                )
                if not page:
                    break
                for rec in page:
                    npi = rec.get("number", "")
                    if not npi or npi in seen_npis:
                        continue
                    seen_npis.add(npi)

                    basic = rec.get("basic") or {}
                    org   = (basic.get("organization_name") or "").strip()
                    if not org:
                        continue

                    # Optional high-intent filter: only orgs enumerated recently
                    enum_date = (basic.get("enumeration_date") or "").strip()
                    if new_only and not _recent(enum_date, days=new_days):
                        continue

                    addrs = rec.get("addresses") or []
                    practice = next(
                        (a for a in addrs if a.get("address_purpose") == "LOCATION"),
                        addrs[0] if addrs else {},
                    )
                    taxes = rec.get("taxonomies") or []
                    primary = next(
                        (t for t in taxes if t.get("primary")),
                        taxes[0] if taxes else {},
                    )

                    # Enforce state on the PRACTICE address, not any address.
                    # NPPES matches any address; we want labs physically in the
                    # requested state.
                    practice_state = (practice.get("state") or "").upper()
                    if practice_state and practice_state != state.upper():
                        continue

                    # Filter out obvious non-labs (transportation cos, holding
                    # LLCs, etc. that got mis-registered under lab taxonomy).
                    if not _looks_like_lab(org, primary.get("desc", "")):
                        continue

                    out.append({
                        "organization_name": org,
                        "npi":      npi,
                        "address":  practice.get("address_1", ""),
                        "city":     practice.get("city", ""),
                        "state":    practice_state or state.upper(),
                        "zip":      practice.get("postal_code", ""),
                        "phone":    practice.get("telephone_number", ""),
                        "taxonomy": primary.get("desc", ""),
                        "enumeration_date": enum_date,
                        "last_updated":     basic.get("last_updated", ""),
                        "authorized_official_first_name":
                            (basic.get("authorized_official_first_name") or "").strip(),
                        "authorized_official_last_name":
                            (basic.get("authorized_official_last_name") or "").strip(),
                        "authorized_official_title":
                            (basic.get("authorized_official_title_or_position") or "").strip(),
                        "authorized_official_phone":
                            (basic.get("authorized_official_telephone_number") or "").strip(),
                    })
                    if len(out) >= limit:
                        break

                if len(page) < 200:
                    break
                skip += 200
                # NPPES hard-caps at skip=1000; stop before we hit it
                if skip >= 1000:
                    break
    return out[:limit]


async def prospect_multi_state(
    states: list[str],
    specialty: str = "all_labs",
    per_state: int = 50,
    new_only: bool = False,
    new_days: int = 90,
) -> list[dict]:
    """Parallel fetch across multiple states. Dedup across the whole batch."""
    tasks = [
        prospect_state(s, specialty=specialty, limit=per_state,
                       new_only=new_only, new_days=new_days)
        for s in states
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    seen: set[str] = set()
    out: list[dict] = []
    for r in results:
        if isinstance(r, Exception):
            continue
        for row in r:
            key = (row.get("npi") or row.get("organization_name", "")).lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
    return out


async def prospect_and_scrub(
    state: str,
    specialty: str = "all_labs",
    limit: int = 50,
    new_only: bool = False,
    dm_only: bool = True,
) -> dict:
    """One-shot: hunt + enrich. Returns the same shape as `scrub_rows`.

    This is the endpoint that turns the tool from "upload a list" into
    "give me 50 fresh Florida tox labs with DM URLs + hooks".

    `dm_only=True` (default) — skips website scraping, email pattern
    generation, and SMTP verification. Pure DM/social-channel output.
    Spam kills email anyway; DMs land. ~10x faster, zero false-positive
    emails, every row has a real human + LinkedIn/FB/IG/X URL + hook.
    Set `dm_only=False` only if you want the legacy email-hunt path.
    """
    prospects = await prospect_state(
        state, specialty=specialty, limit=limit, new_only=new_only,
    )
    if not prospects:
        return {"summary": {"input_rows": 0, "output_rows": 0}, "rows": [], "daily_top_10": []}

    if dm_only:
        result = await _enrich_dm_only(prospects)
    else:
        from app.scrubber import scrub_rows  # local import to avoid cycle
        headers = ["organization_name", "npi", "address", "city", "state",
                   "zip", "phone", "taxonomy", "enumeration_date", "last_updated"]
        result = await scrub_rows(headers, prospects, max_rows=limit)

    result["prospect_source"] = {
        "state": state.upper(),
        "specialty": specialty,
        "new_only": new_only,
        "dm_only": dm_only,
        "fetched": len(prospects),
    }
    return result


# ─── DM-only fast path ────────────────────────────────────────────────
# No website scraping. No SMTP probes. No email pattern generation.
# Pure: NPPES official + social DM URLs + hook + heat. ~10x faster.

_BAD_NAME_TOKENS = {
    # US states (lowercase, no spaces)
    "alabama","alaska","arizona","arkansas","california","colorado",
    "connecticut","delaware","florida","georgia","hawaii","idaho","illinois",
    "indiana","iowa","kansas","kentucky","louisiana","maine","maryland",
    "massachusetts","michigan","minnesota","mississippi","missouri","montana",
    "nebraska","nevada","newhampshire","newjersey","newmexico","newyork",
    "northcarolina","northdakota","ohio","oklahoma","oregon","pennsylvania",
    "rhodeisland","southcarolina","southdakota","tennessee","texas","utah",
    "vermont","virginia","washington","westvirginia","wisconsin","wyoming",
    # Common non-name tokens
    "new","city","county","llc","inc","corp","corporation","company",
    "services","healthcare","medical","clinic","hospital","laboratory",
    "lab","labs","diagnostic","diagnostics","group","center","associates",
    "holdings","partners","main","campus","office","department",
}
_PLACEHOLDER_PHONES = {
    "9009009009","0000000000","1111111111","1234567890",
    "9999999999","5555555555","8008008000","0123456789","1231231234",
}


def _valid_human_name(first: str, last: str) -> bool:
    """True if first+last look like a real human name, not city/state/junk."""
    if not first or not last:
        return False
    f = first.lower().replace(" ", "").replace(".", "")
    l = last.lower().replace(" ", "").replace(".", "")
    if f in _BAD_NAME_TOKENS or l in _BAD_NAME_TOKENS:
        return False
    if not any(c.isalpha() for c in first) or not any(c.isalpha() for c in last):
        return False
    if not any(c.lower() in "aeiouy" for c in first):
        return False
    if not any(c.lower() in "aeiouy" for c in last):
        return False
    return True


def _format_phone(raw: str) -> str:
    digits = "".join(c for c in (raw or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    if digits in _PLACEHOLDER_PHONES or len(set(digits)) <= 2:
        return ""
    return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"


async def _enrich_dm_only(prospects: list[dict]) -> dict:
    """Build DM-ready rows directly from NPPES records — no scraping."""
    from app.social_finder import find_social_profiles, social_outreach_templates
    from app.playbook import (
        personalized_hook, objection_handlers, heat_score,
        enrich_templates_with_hook,
    )
    from rule_intercept import score_lab_lead

    rows: list[dict] = []
    for p in prospects:
        org = (p.get("organization_name") or "").strip()
        if not org:
            continue

        first = (p.get("authorized_official_first_name") or "").strip()
        last = (p.get("authorized_official_last_name") or "").strip()
        title = (p.get("authorized_official_title") or "").strip()
        ao_phone_raw = (p.get("authorized_official_phone") or "").strip()
        org_phone_raw = (p.get("phone") or "").strip()
        state = (p.get("state") or "").strip()
        city = (p.get("city") or "").strip()
        tax = (p.get("taxonomy") or "").strip()
        last_updated = (p.get("last_updated") or "").strip()
        enum_date = (p.get("enumeration_date") or "").strip()

        # Reject city/state-as-name and other junk
        if not _valid_human_name(first, last):
            first, last, title = "", "", ""
        # Reject if the "name" matches the practice city or state
        if first and first.lower() == city.lower():
            first, last, title = "", "", ""
        if last and last.lower() == city.lower():
            first, last, title = "", "", ""

        ao_phone = _format_phone(ao_phone_raw)
        org_phone = _format_phone(org_phone_raw)

        # Lab fit / lead score (deterministic rule engine)
        lab_intel = score_lab_lead(org, lab_type=tax, state=state)
        lead_score = lab_intel.get("score", 0)
        type_detected = lab_intel.get("lab_type_detected", "")

        # Social DM URLs + per-platform templates
        social = await find_social_profiles(first, last, org=org, title=title) or {}
        templates = social_outreach_templates(first or "there", org)

        # Personalized hook + inject into templates
        hook = personalized_hook(
            first, org, taxonomy_desc=tax, lab_type_detected=type_detected,
            state=state, last_updated=last_updated,
        )
        templates = enrich_templates_with_hook(templates, hook)

        # Objection handlers
        objections = objection_handlers(first=first, org=org)

        # Heat score
        has_dm = bool(first and last)
        has_direct_line = bool(ao_phone)
        has_social = bool(social.get("linkedin_url"))
        score, reasons = heat_score(
            lead_score=lead_score,
            fit_score=lab_intel.get("fit_score", 0),
            has_dm=has_dm,
            has_direct_line=has_direct_line,
            has_verified_domain=False,  # we don't probe domains in DM-only
            has_social=has_social,
            last_updated=last_updated,
            state=state,
        )

        full_name = f"{first} {last}".strip().upper() if (first and last) else ""
        rows.append({
            "Heat Score": score,
            "Heat Reasons": "; ".join(reasons),
            "NPI Last Updated": last_updated,
            "Lead Score": lead_score,
            "Tier": lab_intel.get("tier", ""),
            "Priority": lab_intel.get("priority", ""),
            "Personalized Hook": hook,
            "Org Name": org,
            "Taxonomy / Type": tax,
            "Type Detected": type_detected,
            "NPI": p.get("npi", ""),
            "Address": p.get("address", ""),
            "City": city,
            "State": state,
            "ZIP": p.get("zip", ""),
            "Phone": org_phone,
            "Direct Line": ao_phone,
            "Decision Maker": full_name,
            "DM Title": title,
            "DM Email": "",  # DM-only mode — no email
            # Social DM URLs
            "LinkedIn URL": social.get("linkedin_url", ""),
            "LinkedIn Sales Nav URL": social.get("linkedin_sales_nav", ""),
            "Facebook URL": social.get("facebook_url", ""),
            "Instagram URL": social.get("instagram_url", ""),
            "X / Twitter URL": social.get("x_url", ""),
            "Google Social Search": social.get("google_social", ""),
            "Google LinkedIn Search": social.get("google_linkedin", ""),
            "LinkedIn Company Page": social.get("linkedin_company_url", ""),
            "Facebook Company Page": social.get("facebook_page_url", ""),
            "Instagram Company": social.get("instagram_company_url", ""),
            # Paste-ready DM templates
            "LinkedIn Connection Note": templates.get("linkedin_connection_note", ""),
            "LinkedIn First Message": templates.get("linkedin_first_message", ""),
            "LinkedIn Follow-up": templates.get("linkedin_follow_up", ""),
            "Facebook DM": templates.get("facebook_dm", ""),
            "Instagram DM": templates.get("instagram_dm", ""),
            "X / Twitter DM": templates.get("x_dm", ""),
            "SMS Template": templates.get("sms", ""),
            # Objection handlers
            "Reply: Already Have Biller": objections.get("objection_already_have_biller", ""),
            "Reply: Send Info First": objections.get("objection_send_info_first", ""),
            "Reply: What Does It Cost": objections.get("objection_what_does_it_cost", ""),
            "Reply: Not Interested": objections.get("objection_not_interested", ""),
            "Reply: Busy Now": objections.get("objection_busy_now", ""),
            "Reply: Who Are You": objections.get("objection_who_are_you", ""),
            "Enumeration Date": enum_date,
        })

    rows.sort(key=lambda r: -int(r.get("Heat Score") or 0))
    daily_top_10 = rows[:10]

    summary = {
        "input_rows": len(prospects),
        "output_rows": len(rows),
        "rows_with_dm": sum(1 for r in rows if r.get("Decision Maker")),
        "rows_with_direct_line": sum(1 for r in rows if r.get("Direct Line")),
        "rows_with_social_dm": sum(1 for r in rows if r.get("LinkedIn URL")),
        "rows_top_heat": sum(1 for r in rows if int(r.get("Heat Score") or 0) >= 70),
        "mode": "dm_only",
    }
    return {"summary": summary, "rows": rows, "daily_top_10": daily_top_10}
