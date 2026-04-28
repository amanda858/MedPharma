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
from app.linkedin_resolver import (
    resolve_linkedin_profile,
    resolve_facebook_profile,
    resolve_instagram_profile,
    resolve_company_linkedin,
    resolve_employee_at_company,
    reset_run_budget,
    linkedin_search_url,
    linkedin_company_search_url,
    linkedin_company_people_url,
)
from app.backup_people import find_backup_people

# Live LinkedIn slug resolution is unreliable from cloud IPs (search engines
# block scrapers). When this flag is False we skip live lookups entirely
# and rely on guaranteed-clickable Bing search URLs (pre-filtered to the
# exact person + org on linkedin.com/in). Result: 100% of rows get a
# one-click path to the LinkedIn profile, in seconds, no rate-limit risk.
import os as _os
LIVE_LINKEDIN_LOOKUP = _os.environ.get("LIVE_LINKEDIN_LOOKUP", "0") == "1"

# Email enrichment (Hunter.io + pattern). On by default — this is the
# single most valuable signal for outreach. Set ENABLE_EMAIL_ENRICHMENT=0
# to skip (e.g. for fast hunts).
ENABLE_EMAIL_ENRICHMENT = _os.environ.get("ENABLE_EMAIL_ENRICHMENT", "1") == "1"


# Convenient specialty groups — each maps to one or more taxonomy keywords.
# Using the `taxonomy_description` query param against NPPES (free-text).
SPECIALTY_KEYWORDS: dict[str, list[str]] = {
    "clinical":        ["clinical medical laboratory"],
    "toxicology":      ["clinical medical laboratory/toxicology",
                        "toxicology",
                        "clinical medical laboratory"],   # fallback widens net
    "pathology":       ["pathology"],
    "molecular":       ["clinical medical laboratory/molecular",
                        "molecular", "genetic"],
    "genetic":         ["clinical medical laboratory/molecular",
                        "genetic", "genomic"],
    "blood_bank":      ["clinical medical laboratory/blood banking",
                        "blood bank"],
    "cytopathology":   ["pathology/cytopathology", "cytopathology", "cytology"],
    "histology":       ["pathology/histology", "histology"],
    "microbiology":    ["clinical medical laboratory/microbiology",
                        "microbiology",
                        "clinical medical laboratory"],   # fallback widens net
    "physiological":   ["physiological laboratory"],
    "physician_office":["clinical medical laboratory/clinical chemistry",
                        "clinical medical laboratory",     # POL is usually here
                        "physician office"],
    "urgent_care":     ["urgent care"],
    "all_labs":        ["clinical medical laboratory",
                        "pathology",
                        "laboratory"],  # broadest net
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

    # Reset the per-run live-lookup budget so each hunt gets a fresh quota
    reset_run_budget()

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

        # ── REAL Email enrichment (website scraping + Hunter only) ─────────
        # NO pattern guesses. We only emit emails actually scraped from
        # the company's live website or returned by Hunter.io.
        # Person-level emails (matched to DM by name) → DM Email.
        # Generic mailboxes (info@, contact@, sales@) → Company Email.
        dm_email = ""
        dm_email_confidence = 0
        dm_email_source = ""
        company_email = ""
        company_email_source = ""
        org_domain = ""
        org_emails: list[dict] = []
        if ENABLE_EMAIL_ENRICHMENT:
            try:
                from app.email_finder import find_emails_for_lab
                em = await find_emails_for_lab(
                    org_name=org, first_name=first, last_name=last,
                )
                org_domain = em.get("live_domain", "") or ""
                org_emails = em.get("emails") or []
                # Drop any pattern-generated emails defensively (belt & suspenders).
                org_emails = [
                    e for e in org_emails
                    if (e.get("source") or "") != "pattern_generated"
                ]
                tgt_first = (first or "").lower()
                tgt_last = (last or "").lower()
                # Person-level: match name OR (named, decision-maker, not generic)
                person_pool = [e for e in org_emails if not e.get("is_generic")]
                best_person = None
                if tgt_first and tgt_last:
                    for e in person_pool:
                        if (e.get("first_name", "").lower() == tgt_first
                                and e.get("last_name", "").lower() == tgt_last):
                            best_person = e
                            break
                if not best_person:
                    for e in person_pool:
                        if e.get("is_decision_maker") and e.get("first_name"):
                            best_person = e
                            break
                if not best_person and person_pool:
                    # Take a named person if available
                    for e in person_pool:
                        if e.get("first_name") or e.get("last_name"):
                            best_person = e
                            break
                if not best_person and person_pool:
                    # Last resort: any real non-generic scraped email at this domain
                    best_person = person_pool[0]
                if best_person:
                    dm_email = best_person.get("email", "")
                    dm_email_confidence = int(best_person.get("confidence", 0) or 0)
                    dm_email_source = best_person.get("source", "")
                # Company-level mailbox (info@/contact@/sales@)
                generic_pool = [e for e in org_emails if e.get("is_generic")]
                # Prefer info@ > contact@ > sales@ > rest
                pref_order = ["info", "contact", "sales", "office", "admin", "hello"]
                generic_pool.sort(key=lambda e: pref_order.index(
                    (e.get("email", "").split("@", 1)[0] or "").lower()
                ) if (e.get("email", "").split("@", 1)[0] or "").lower() in pref_order else 99)
                if generic_pool:
                    company_email = generic_pool[0].get("email", "")
                    company_email_source = generic_pool[0].get("source", "")
            except Exception:
                pass

        # ── Real-profile resolver (off by default — cloud IPs blocked) ─────
        # When LIVE_LINKEDIN_LOOKUP=1 we'll try to resolve direct slugs.
        # Otherwise we skip straight to guaranteed-clickable search URLs.
        if first and last and LIVE_LINKEDIN_LOOKUP:
            real_li = resolve_linkedin_profile(first, last, org)
            real_fb = resolve_facebook_profile(first, last, org)
            real_ig = resolve_instagram_profile(first, last, org)
        else:
            real_li = real_fb = real_ig = ""
        # Fallbacks: when the named DM has no LinkedIn, surface the company
        # page + up to 3 verified employee profiles so the user still has
        # a real human at the org to DM.
        company_li = resolve_company_linkedin(org) if (org and LIVE_LINKEDIN_LOOKUP) else ""
        employee_lis: list[str] = []
        li_label = "DM" if (first and last) else ""
        if not real_li and org and LIVE_LINKEDIN_LOOKUP:
            employee_lis = resolve_employee_at_company(org, max_results=3)
            if employee_lis:
                real_li = employee_lis[0]
                li_label = "Employee"

        # ── Always-clickable LinkedIn search URLs (100% hit rate) ──────────
        # Pre-filtered Bing searches that land on LinkedIn results for
        # this exact person + org. Production fallback for every row.
        li_search_dm = linkedin_search_url(first, last, org) if (first and last) else ""
        li_search_company = linkedin_company_search_url(org) if org else ""
        li_company_people = linkedin_company_people_url(org) if org else ""

        # ── NPPES backup person (free, unlimited, reliable) ────────────────────────
        # Always pull a backup person at the same address — even when DM
        # has good contact info, the backup adds optionality.
        backup_first = backup_last = backup_title = backup_phone = backup_npi = ""
        backup_li = ""
        backup_li_search = ""
        try:
            backups = await find_backup_people(
                zip_code=p.get("zip", ""),
                city=city,
                state=state,
                street_address=p.get("address", ""),
                exclude_npi=p.get("npi", ""),
                limit=3,
            )
        except Exception:
            backups = []
        if backups:
            cand = backups[0]
            backup_first = cand["first"]
            backup_last = cand["last"]
            backup_title = cand.get("title", "") or cand.get("taxonomy", "")
            backup_phone = _format_phone(cand.get("phone", ""))
            backup_npi = cand.get("npi", "")
            backup_li_search = linkedin_search_url(backup_first, backup_last, org)
            if LIVE_LINKEDIN_LOOKUP:
                backup_li = resolve_linkedin_profile(backup_first, backup_last, org)
        # Personalized hook + inject into templates
        hook = personalized_hook(
            first, org, taxonomy_desc=tax, lab_type_detected=type_detected,
            state=state, last_updated=last_updated, city=city,
        )
        templates = enrich_templates_with_hook(templates, hook)

        # Objection handlers
        objections = objection_handlers(first=first, org=org)

        # Heat score
        has_dm = bool(first and last)
        has_direct_line = bool(ao_phone)
        has_social = bool(real_li or real_fb or real_ig)
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
            "DM Email": dm_email,
            "DM Email Confidence": dm_email_confidence,
            "DM Email Source": dm_email_source,
            "Company Email": company_email,
            "Company Email Source": company_email_source,
            "Org Domain": org_domain,
            "Org Emails Found": "; ".join(e.get("email","") for e in org_emails[:5]),
            # Social DM URLs (real_li only when verified; otherwise blank)
            "LinkedIn URL": real_li,
            "LinkedIn Match Type": li_label if real_li else "",
            "LinkedIn Search URL": li_search_dm,
            "LinkedIn Sales Nav URL": social.get("linkedin_sales_nav", "") if real_li else "",
            "Facebook URL": real_fb,
            "Instagram URL": real_ig,
            "X / Twitter URL": "",  # Don't speculate
            "Google Social Search": "",
            "Google LinkedIn Search": "",
            "LinkedIn Company Page": company_li,
            "LinkedIn Company Search URL": li_search_company,
            "LinkedIn Company Roster URL": li_company_people,
            "LinkedIn Other Employees": " | ".join(employee_lis[1:]) if len(employee_lis) > 1 else "",
            "Facebook Company Page": "",
            "Instagram Company": "",
            # Backup person at same address (NPPES NPI-1 lookup)
            "Backup Contact": f"{backup_first} {backup_last}".strip(),
            "Backup Title": backup_title,
            "Backup Phone": backup_phone,
            "Backup NPI": backup_npi,
            "Backup LinkedIn": backup_li,
            "Backup LinkedIn Search URL": backup_li_search,
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

    # Production filter: drop rows with no reachable human at all.
    # A row needs at least ONE of:
    #   - a real DM name (from NPPES authorized official) AND any phone, OR
    #   - a qualified backup person (NPPES NPI-1 with phone)
    # Otherwise it's noise and wastes the user's outreach time.
    def _has_reach(r: dict) -> bool:
        has_dm_reach = bool(r.get("Decision Maker") and (r.get("Direct Line") or r.get("Phone") or r.get("DM Email")))
        has_email = bool(r.get("DM Email") or r.get("Company Email"))
        has_backup_reach = bool(r.get("Backup Contact") and r.get("Backup Phone"))
        has_social = bool(r.get("LinkedIn URL") or r.get("Facebook URL") or r.get("Instagram URL")
                          or r.get("LinkedIn Search URL") or r.get("Backup LinkedIn Search URL"))
        return has_dm_reach or has_email or has_backup_reach or has_social

    pre_filter = len(rows)
    rows = [r for r in rows if _has_reach(r)]
    dropped = pre_filter - len(rows)

    rows.sort(key=lambda r: -int(r.get("Heat Score") or 0))
    daily_top_10 = rows[:10]

    summary = {
        "input_rows": len(prospects),
        "output_rows": len(rows),
        "rows_dropped_no_reach": dropped,
        "rows_with_dm": sum(1 for r in rows if r.get("Decision Maker")),
        "rows_with_direct_line": sum(1 for r in rows if r.get("Direct Line")),
        "rows_with_backup": sum(1 for r in rows if r.get("Backup Contact")),
        "rows_with_social_dm": sum(1 for r in rows if r.get("LinkedIn URL")),
        "rows_top_heat": sum(1 for r in rows if int(r.get("Heat Score") or 0) >= 70),
        "mode": "dm_only",
    }
    return {"summary": summary, "rows": rows, "daily_top_10": daily_top_10}
