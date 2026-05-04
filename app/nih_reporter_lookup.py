"""NIH Reporter funded project PI lookup.

Lab directors and pathologists who receive NIH grants are among the highest-quality
targets for RCM/auditing services — they run high-volume, complex labs.
The NIH Reporter API is free, no key required, and returns PI name + institution.

We can derive the institutional email from the PI name + org domain using the
standard academic pattern (first.last@university.edu), then MX-verify it.
"""
from __future__ import annotations
import asyncio
import httpx

BASE = "https://api.reporter.nih.gov/v2/projects/search"
TIMEOUT = 8.0


async def find_nih_pi(
    org_name: str,
    first_name: str = "",
    last_name: str = "",
    state: str = "",
    max_results: int = 5,
) -> list[dict]:
    """Search NIH Reporter for funded PIs at this org or matching this person.

    Returns list of dicts: {"first", "last", "title", "org_name", "org_state",
                             "dept", "profile_id", "source"}
    No email directly — caller combines with domain lookup.
    """
    try:
        criteria: dict = {}
        if first_name and last_name:
            criteria["pi_names"] = [{"any_name": f"{first_name} {last_name}"}]
        elif org_name:
            criteria["org_names"] = [org_name]
        if state:
            criteria["org_states"] = [state.upper()]

        if not criteria:
            return []

        payload = {
            "criteria": criteria,
            "include_fields": ["principal_investigators", "organization", "project_title"],
            "offset": 0,
            "limit": max_results,
        }
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            r = await client.post(BASE, json=payload)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return []

    results = []
    seen = set()
    for proj in data.get("results", []):
        org = proj.get("organization", {})
        for pi in proj.get("principal_investigators", []):
            key = (pi.get("first_name", ""), pi.get("last_name", ""))
            if key in seen:
                continue
            seen.add(key)
            results.append({
                "first": pi.get("first_name", "").strip().title(),
                "last": pi.get("last_name", "").strip().title(),
                "title": pi.get("title", ""),
                "org_name": org.get("org_name", ""),
                "org_state": org.get("org_state", ""),
                "dept": org.get("dept_type", ""),
                "profile_id": pi.get("profile_id"),
                "source": "nih_reporter",
            })
    return results
