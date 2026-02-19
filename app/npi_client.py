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
