"""CMS Open Payments (Sunshine Act) physician lookup.

Labs and lab directors who receive payments from pharma/device companies are
publicly listed. This gives us verified names, specialties, and NPI numbers
for high-value targets.

API: https://openpaymentsdata.cms.gov  (free, no key)
NAICS equivalent: physicians who order lab work
"""
from __future__ import annotations
import asyncio
import httpx

# Physician profile dataset (annual, includes NPI + specialty + state)
PHYSICIAN_API = (
    "https://openpaymentsdata.cms.gov/api/1/datastore/query/"
    "6ed6ae76-2999-49da-b0b2-d7df150ac754/0"
)
TIMEOUT = 8.0


async def find_open_payments_physician(
    first_name: str,
    last_name: str,
    state: str = "",
) -> list[dict]:
    """Look up a physician in the CMS Open Payments dataset.

    Returns list of dicts: {"first", "last", "specialty", "city", "state",
                             "npi", "license_state", "source"}
    Confirms the person is real and gives their specialty + location.
    """
    if not first_name or not last_name:
        return []
    try:
        params: dict = {
            "conditions[0][property]": "Physician_First_Name",
            "conditions[0][value]": first_name.upper(),
            "conditions[0][operator]": "=",
            "conditions[1][property]": "Physician_Last_Name",
            "conditions[1][value]": last_name.upper(),
            "conditions[1][operator]": "=",
            "limit": 5,
        }
        if state:
            params.update({
                "conditions[2][property]": "Physician_License_State_code1",
                "conditions[2][value]": state.upper(),
                "conditions[2][operator]": "=",
            })
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            r = await client.get(PHYSICIAN_API, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return []

    results = []
    seen_npi = set()
    for row in data.get("results", []):
        npi = row.get("Physician_NPI", "")
        if npi in seen_npi:
            continue
        seen_npi.add(npi)
        specialty = row.get("Physician_Specialty", "")
        results.append({
            "first": row.get("Physician_First_Name", "").strip().title(),
            "last": row.get("Physician_Last_Name", "").strip().title(),
            "specialty": specialty,
            "city": row.get("Physician_City", "").strip().title(),
            "state": row.get("Physician_License_State_code1", ""),
            "npi": npi,
            "source": "cms_open_payments",
        })
    return results
