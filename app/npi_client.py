"""NPI Registry API client — free, no API key required."""

import httpx
import asyncio
from typing import Optional
from app.config import NPI_API_BASE, NPI_API_VERSION, LAB_TAXONOMY_CODES


async def search_npi(
    state: Optional[str] = None,
    city: Optional[str] = None,
    organization_name: Optional[str] = None,
    taxonomy_description: Optional[str] = None,
    postal_code: Optional[str] = None,
    limit: int = 50,
    skip: int = 0,
    enumeration_type: str = "NPI-2",  # NPI-2 = organizations
) -> dict:
    """
    Search the NPPES NPI Registry for laboratory providers.

    Args:
        state: 2-letter state code (e.g., 'TX', 'CA')
        city: City name
        organization_name: Organization name (partial match)
        taxonomy_description: Taxonomy description keyword (e.g., 'laboratory')
        postal_code: ZIP code (5-digit or 9-digit)
        limit: Number of results (max 200)
        skip: Number of results to skip for pagination
        enumeration_type: NPI-1 (individual) or NPI-2 (organization)

    Returns:
        Dict with results and result_count
    """
    params = {
        "version": NPI_API_VERSION,
        "limit": min(limit, 200),
        "skip": skip,
        "enumeration_type": enumeration_type,
    }

    if state:
        params["state"] = state.upper()
    if city:
        params["city"] = city
    if organization_name:
        params["organization_name"] = f"*{organization_name}*"
    if taxonomy_description:
        params["taxonomy_description"] = taxonomy_description
    if postal_code:
        params["postal_code"] = postal_code

    # Default search for labs if no taxonomy specified
    if not taxonomy_description:
        params["taxonomy_description"] = "laboratory"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(NPI_API_BASE, params=params)
        response.raise_for_status()
        data = response.json()

    return _parse_npi_results(data)


async def search_npi_by_taxonomy(
    taxonomy_code: str,
    state: Optional[str] = None,
    limit: int = 50,
    skip: int = 0,
) -> dict:
    """Search NPI registry by specific taxonomy code."""
    params = {
        "version": NPI_API_VERSION,
        "limit": min(limit, 200),
        "skip": skip,
        "enumeration_type": "NPI-2",
        "taxonomy_description": _get_taxonomy_desc(taxonomy_code),
    }

    if state:
        params["state"] = state.upper()

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(NPI_API_BASE, params=params)
        response.raise_for_status()
        data = response.json()

    return _parse_npi_results(data)


async def get_npi_detail(npi: str) -> dict:
    """Get detailed information for a specific NPI number."""
    params = {
        "version": NPI_API_VERSION,
        "number": npi,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(NPI_API_BASE, params=params)
        response.raise_for_status()
        data = response.json()

    results = _parse_npi_results(data)
    if results["results"]:
        return results["results"][0]
    return {}


async def bulk_search_labs(
    states: list[str],
    limit_per_state: int = 50,
) -> dict:
    """Search for labs across multiple states concurrently."""
    tasks = []
    for state in states:
        tasks.append(search_npi(state=state, limit=limit_per_state))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_labs = []
    total = 0
    errors = []

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            errors.append({"state": states[i], "error": str(result)})
        else:
            all_labs.extend(result["results"])
            total += result["result_count"]

    return {
        "results": all_labs,
        "result_count": total,
        "states_searched": len(states),
        "errors": errors,
    }


def _parse_npi_results(data: dict) -> dict:
    """Parse NPI API response into clean lead records."""
    result_count = data.get("result_count", 0)
    results = []

    if result_count == 0:
        return {"results": [], "result_count": 0}

    for record in data.get("results", []):
        basic = record.get("basic", {})
        addresses = record.get("addresses", [])
        taxonomies = record.get("taxonomies", [])

        # Get primary practice address
        practice_addr = {}
        mailing_addr = {}
        for addr in addresses:
            if addr.get("address_purpose") == "LOCATION":
                practice_addr = addr
            elif addr.get("address_purpose") == "MAILING":
                mailing_addr = addr

        # Use practice address first, fall back to mailing
        addr = practice_addr or mailing_addr

        # Get primary taxonomy
        primary_tax = {}
        for tax in taxonomies:
            if tax.get("primary", False):
                primary_tax = tax
                break
        if not primary_tax and taxonomies:
            primary_tax = taxonomies[0]

        # Calculate a lead score based on available info
        lead_score = _calculate_lead_score(record)

        lead = {
            "npi": record.get("number", ""),
            "entity_type": "Organization" if basic.get("organization_name") else "Individual",
            "organization_name": basic.get("organization_name", ""),
            "first_name": basic.get("first_name", ""),
            "last_name": basic.get("last_name", ""),
            "credential": basic.get("credential", ""),
            "taxonomy_code": primary_tax.get("code", ""),
            "taxonomy_desc": primary_tax.get("desc", ""),
            "taxonomy_license": primary_tax.get("license", ""),
            "taxonomy_state": primary_tax.get("state", ""),
            "address_line1": addr.get("address_1", ""),
            "address_line2": addr.get("address_2", ""),
            "city": addr.get("city", ""),
            "state": addr.get("state", ""),
            "zip_code": addr.get("postal_code", ""),
            "phone": _format_phone(addr.get("telephone_number", "")),
            "fax": _format_phone(addr.get("fax_number", "")),
            "enumeration_date": basic.get("enumeration_date", ""),
            "last_updated": basic.get("last_updated", ""),
            "lead_score": lead_score,
            "all_taxonomies": [
                {"code": t.get("code", ""), "desc": t.get("desc", "")}
                for t in taxonomies
            ],
        }
        results.append(lead)

    return {
        "results": results,
        "result_count": result_count,
    }


def _calculate_lead_score(record: dict) -> int:
    """
    Score a lead 0-100 based on likelihood they need billing/compliance services.
    Higher = more likely to need services.
    """
    score = 50  # Base score

    basic = record.get("basic", {})
    taxonomies = record.get("taxonomies", [])
    addresses = record.get("addresses", [])

    # Organization labs are better targets than individual providers
    if basic.get("organization_name"):
        score += 15

    # Clinical medical laboratories are prime targets
    for tax in taxonomies:
        code = tax.get("code", "")
        if code == "291U00000X":  # Clinical Medical Laboratory
            score += 20
        elif code.startswith("291") or code.startswith("292") or code.startswith("293"):
            score += 10

    # Labs with phone numbers are more reachable
    for addr in addresses:
        if addr.get("telephone_number"):
            score += 5
            break

    # Recently updated records suggest active operation
    last_updated = basic.get("last_updated", "")
    if last_updated and last_updated >= "2024":
        score += 10
    elif last_updated and last_updated >= "2023":
        score += 5

    return min(score, 100)


def _format_phone(phone: str) -> str:
    """Format phone number."""
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def _get_taxonomy_desc(code: str) -> str:
    """Get taxonomy description from code."""
    return LAB_TAXONOMY_CODES.get(code, "laboratory")


async def find_org_official(
    organization_name: str,
    state: Optional[str] = None,
    city: Optional[str] = None,
) -> Optional[dict]:
    """Look up the authorized official (real human name + title + phone)
    for a healthcare organization in the NPPES NPI registry. Free, no key.

    Returns {first, last, middle, title, phone, npi, taxonomy, address, city,
    state, zip, org_name} for the best fuzzy match, or None.
    """
    if not organization_name or len(organization_name.strip()) < 2:
        return None

    params = {
        "version": NPI_API_VERSION,
        "limit": 5,
        "skip": 0,
        "enumeration_type": "NPI-2",
        "organization_name": f"*{organization_name.strip()}*",
    }
    if state:
        params["state"] = state.upper()
    if city:
        params["city"] = city

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(NPI_API_BASE, params=params)
            response.raise_for_status()
            data = response.json()
    except Exception:
        return None

    results = data.get("results") or []
    if not results:
        return None

    # Pick the best match: shortest name distance from the input
    target = organization_name.strip().lower()
    best = None
    best_score = -1
    for rec in results:
        basic = rec.get("basic") or {}
        org = (basic.get("organization_name") or "").strip()
        if not org:
            continue
        org_lc = org.lower()
        # Score: substring match wins; otherwise prefix overlap length
        if target in org_lc or org_lc in target:
            score = 100 - abs(len(org_lc) - len(target))
        else:
            common = 0
            for a, b in zip(target, org_lc):
                if a != b:
                    break
                common += 1
            score = common
        if score > best_score:
            best_score = score
            best = rec

    if not best:
        return None

    # Reject low-quality matches — fuzzy match can return totally
    # unrelated orgs if nothing in the result set is close.
    if best_score < 5:
        return None

    basic = best.get("basic") or {}
    first = (basic.get("authorized_official_first_name") or "").strip()
    last = (basic.get("authorized_official_last_name") or "").strip()
    if not first and not last:
        return None

    # Reject obviously-not-a-name values that NPPES sometimes stores
    # in the authorized_official fields (city names, state names, etc.).
    _US_STATE_NAMES = {
        "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
        "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
        "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
        "maine", "maryland", "massachusetts", "michigan", "minnesota",
        "mississippi", "missouri", "montana", "nebraska", "nevada",
        "newhampshire", "newjersey", "newmexico", "newyork", "northcarolina",
        "northdakota", "ohio", "oklahoma", "oregon", "pennsylvania",
        "rhodeisland", "southcarolina", "southdakota", "tennessee", "texas",
        "utah", "vermont", "virginia", "washington", "westvirginia",
        "wisconsin", "wyoming",
    }
    _BAD_NAME_TOKENS = _US_STATE_NAMES | {
        "new", "city", "county", "avenue", "street", "road", "boulevard",
        "llc", "inc", "corp", "corporation", "company", "services",
        "healthcare", "medical", "clinic", "hospital", "laboratory",
        "lab", "labs", "diagnostic", "diagnostics", "group", "center",
        "associates", "holdings", "partners",
    }
    _first_lc = first.lower().replace(" ", "").replace(".", "")
    _last_lc = last.lower().replace(" ", "").replace(".", "")
    if _first_lc in _BAD_NAME_TOKENS or _last_lc in _BAD_NAME_TOKENS:
        return None
    # Names should contain at least one vowel and be alpha-ish
    if first and not any(c.isalpha() for c in first):
        return None
    if last and not any(c.isalpha() for c in last):
        return None
    # Must have vowels — real names do
    if first and not any(c.lower() in "aeiouy" for c in first):
        return None
    if last and not any(c.lower() in "aeiouy" for c in last):
        return None

    # Extract practice address + primary taxonomy for context
    addresses = best.get("addresses") or []
    practice = next(
        (a for a in addresses if a.get("address_purpose") == "LOCATION"),
        addresses[0] if addresses else {},
    )

    # Geographic sanity: if first or last name is the same as the
    # practice city or state, NPPES has city/state mis-filed as a
    # person name (classic data-entry bug). Reject.
    _addr_city = (practice.get("city") or "").strip().lower().replace(" ", "")
    _addr_state_full = (practice.get("state") or "").strip().lower()
    if _first_lc and _first_lc == _addr_city:
        return None
    if _last_lc and _last_lc == _addr_city:
        return None
    if _first_lc in {_addr_state_full, _addr_state_full.replace(" ", "")}:
        return None
    if _last_lc in {_addr_state_full, _addr_state_full.replace(" ", "")}:
        return None

    # Reject placeholder phones like (900) 900-9009, (000) 000-0000, 123-456-7890
    _raw_phone = (basic.get("authorized_official_telephone_number") or "")
    _digits = "".join(c for c in _raw_phone if c.isdigit())
    _placeholder_phones = {
        "9009009009", "0000000000", "1111111111", "1234567890",
        "9999999999", "5555555555",
    }
    if _digits in _placeholder_phones:
        # Clear the phone but keep the record — still usable via other channels
        basic["authorized_official_telephone_number"] = ""
    taxonomies = best.get("taxonomies") or []
    primary_tax = next((t for t in taxonomies if t.get("primary")), taxonomies[0] if taxonomies else {})

    return {
        "npi": best.get("number", ""),
        "org_name": basic.get("organization_name", ""),
        "first": first,
        "last": last,
        "middle": (basic.get("authorized_official_middle_name") or "").strip(),
        "title": (basic.get("authorized_official_title_or_position") or "").strip(),
        "phone": _format_phone(basic.get("authorized_official_telephone_number") or ""),
        "taxonomy_code": primary_tax.get("code", ""),
        "taxonomy_desc": primary_tax.get("desc", ""),
        "address": practice.get("address_1", ""),
        "city": practice.get("city", ""),
        "state": practice.get("state", ""),
        "zip": practice.get("postal_code", ""),
        "last_updated": (basic.get("last_updated") or "").strip(),
        "enumeration_date": (basic.get("enumeration_date") or "").strip(),
        "match_score": best_score,
    }


def person_email_patterns(first: str, last: str, domain: str) -> list[str]:
    """Generate the canonical set of person-specific email patterns.

    Returns a deduped list ordered roughly by industry frequency
    (firstname.lastname is the most common corporate format).
    """
    if not first or not last or not domain:
        return []
    f = first.strip().lower().replace(" ", "")
    l = last.strip().lower().replace(" ", "").replace("-", "")
    d = domain.strip().lower().lstrip("@").lstrip(".")
    if not f or not l or not d:
        return []
    fi = f[0]
    li = l[0]
    seen: set[str] = set()
    out: list[str] = []
    for local in (
        f"{f}.{l}",       # john.smith
        f"{fi}{l}",       # jsmith
        f"{f}{l}",        # johnsmith
        f"{f}_{l}",       # john_smith
        f"{f}-{l}",       # john-smith
        f"{f}",           # john
        f"{l}.{f}",       # smith.john
        f"{l}{fi}",       # smithj
        f"{f}.{li}",      # john.s
        f"{fi}.{l}",      # j.smith
        f"{l}",           # smith
    ):
        addr = f"{local}@{d}"
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out

