"""ClinicalTrials.gov contact email lookup.

Lab directors, pathologists, and clinical lab directors running studies
list their REAL institutional email as the study contact. This is one of
the few truly free, publicly-consented sources of healthcare professional
email addresses.

API: https://clinicaltrials.gov/api/v2  (no key required)
Rate limit: ~10 req/s — well within normal usage
"""

from __future__ import annotations
import asyncio
import httpx

BASE = "https://clinicaltrials.gov/api/v2/studies"
FIELDS = "NCTId,CentralContactName,CentralContactEMail,CentralContactRole,OverallOfficialName,OverallOfficialAffiliation,OverallOfficialRole"
TIMEOUT = 6.0


async def find_clinicaltrials_email(
    org_name: str,
    first_name: str = "",
    last_name: str = "",
    max_results: int = 10,
) -> list[dict]:
    """Search ClinicalTrials.gov for studies associated with this org/person.

    Returns list of dicts: {"name", "email", "role", "affiliation", "source"}
    Sorted by preference: exact name match > org match > any match.
    Only returns rows with real email addresses.
    """
    results: list[dict] = []
    seen_emails: set[str] = set()

    async def _fetch(query: str) -> list[dict]:
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                r = await client.get(BASE, params={
                    "query.term": query,
                    "fields": FIELDS,
                    "pageSize": max_results,
                })
                r.raise_for_status()
                data = r.json()
        except Exception:
            return []

        hits = []
        for study in data.get("studies", []):
            ps = study.get("protocolSection", {})
            contacts = ps.get("contactsLocationsModule", {}).get("centralContacts", [])
            officials = ps.get("contactsLocationsModule", {}).get("overallOfficials", [])

            for c in contacts + officials:
                email = (c.get("email") or c.get("eMail") or "").strip().lower()
                name = (c.get("name") or "").strip()
                role = (c.get("role") or "").strip()
                affil = (c.get("affiliation") or "").strip()

                if not email or "@" not in email:
                    continue
                # Skip generic addresses
                generic_prefixes = ("info@", "contact@", "study@", "admin@", "research@", "trials@")
                if any(email.startswith(p) for p in generic_prefixes):
                    continue
                if email in seen_emails:
                    continue
                seen_emails.add(email)
                hits.append({
                    "name": name,
                    "email": email,
                    "role": role,
                    "affiliation": affil,
                    "source": "clinicaltrials",
                })
        return hits

    # Strategy 1: search by person name + org
    if first_name and last_name and org_name:
        q = f'"{first_name} {last_name}" {org_name}'
        results.extend(await _fetch(q))

    # Strategy 2: person name alone (catches name variants)
    if first_name and last_name and not results:
        q = f'"{first_name} {last_name}"'
        r2 = await _fetch(q)
        # Filter to org-matching results only
        org_lower = org_name.lower()
        results.extend([h for h in r2 if org_lower[:8] in h.get("affiliation", "").lower()])

    # Strategy 3: org name alone (gets any contact at that org)
    if org_name and not results:
        results.extend(await _fetch(org_name))

    # Rank: exact name match first
    full_name = f"{first_name} {last_name}".strip().lower()
    def _rank(h: dict) -> int:
        n = h.get("name", "").lower()
        if full_name and full_name in n:
            return 0
        if org_name.lower()[:8] in h.get("affiliation", "").lower():
            return 1
        return 2

    results.sort(key=_rank)
    return results[:5]
