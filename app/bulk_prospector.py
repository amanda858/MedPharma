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
) -> dict:
    """One-shot: hunt + enrich. Returns the same shape as `scrub_rows`.

    This is the endpoint that turns the tool from "upload a list" into
    "give me 50 fresh Florida tox labs with DM URLs + hooks".
    """
    from app.scrubber import scrub_rows  # local import to avoid cycle

    prospects = await prospect_state(
        state, specialty=specialty, limit=limit, new_only=new_only,
    )
    if not prospects:
        return {"summary": {"input_rows": 0, "output_rows": 0}, "rows": [], "daily_top_10": []}

    headers = ["organization_name", "npi", "address", "city", "state",
               "zip", "phone", "taxonomy", "enumeration_date", "last_updated"]
    result = await scrub_rows(headers, prospects, max_rows=limit)
    result["prospect_source"] = {
        "state": state.upper(),
        "specialty": specialty,
        "new_only": new_only,
        "fetched": len(prospects),
    }
    return result
