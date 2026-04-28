"""NPPES-based "backup person" finder.

When the named decision-maker can't be found on social media, the next-best
backup is *another real human registered at the same practice address*.
NPPES exposes both NPI-2 (organization) and NPI-1 (individual practitioner)
records. Individual practitioners list their practice address — querying
``enumeration_type=NPI-1`` with the same postal_code + city + state surfaces
every doctor / NP / PA / pathologist registered at that lab. These are the
real people who run the place.

Usage:
    backups = await find_backup_people(zip_code="33458", city="JUPITER",
                                        state="FL", exclude_npi="1234567890")
    # → [{"first": "Jane", "last": "Doe", "title": "Pathologist",
    #     "phone": "5615551234", "npi": "..."}]

Reliable, free, no rate limits. Cached per (zip,city,state) so we don't
hammer NPPES.
"""

from __future__ import annotations

import os
import sqlite3
import time
import urllib.parse
from typing import Optional

import httpx

CACHE_DB = os.environ.get("BACKUP_PEOPLE_CACHE", "/tmp/backup_people_cache.db")
CACHE_TTL_SEC = 24 * 3600

NPI_API = "https://npiregistry.cms.hhs.gov/api/"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(CACHE_DB, timeout=5)
    c.execute(
        "CREATE TABLE IF NOT EXISTS backup_people("
        "key TEXT PRIMARY KEY, payload TEXT, fetched_at INTEGER)"
    )
    return c


def _cache_get(key: str) -> Optional[str]:
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT payload, fetched_at FROM backup_people WHERE key=?", (key,)
            ).fetchone()
            if not row:
                return None
            payload, ts = row
            if (time.time() - (ts or 0)) > CACHE_TTL_SEC:
                return None
            return payload
    except Exception:
        return None


def _cache_put(key: str, payload: str) -> None:
    try:
        with _conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO backup_people(key, payload, fetched_at) VALUES (?,?,?)",
                (key, payload, int(time.time())),
            )
    except Exception:
        pass


async def find_backup_people(
    zip_code: str = "",
    city: str = "",
    state: str = "",
    exclude_npi: str = "",
    limit: int = 5,
) -> list[dict]:
    """Return up to ``limit`` individual practitioners (NPI-1) at the same address.

    Excludes ``exclude_npi`` so we don't return the org itself. Each entry has
    first, last, title, phone, npi.
    """
    if not (zip_code or (city and state)):
        return []
    key = f"{(zip_code or '').strip()}|{(city or '').strip().upper()}|{(state or '').strip().upper()}"
    cached = _cache_get(key)
    if cached is not None:
        try:
            import json as _j
            return [p for p in _j.loads(cached) if p.get("npi") != exclude_npi][:limit]
        except Exception:
            pass

    params = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "limit": 50,
    }
    if zip_code:
        params["postal_code"] = zip_code.strip()[:5]
    if state:
        params["state"] = state.strip().upper()
    if city:
        params["city"] = city.strip()

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(NPI_API, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return []

    out: list[dict] = []
    for rec in (data.get("results") or []):
        npi = rec.get("number", "")
        if not npi:
            continue
        basic = rec.get("basic") or {}
        first = (basic.get("first_name") or "").strip()
        last = (basic.get("last_name") or "").strip()
        if not (first and last):
            continue
        # Skip junk
        if first.lower() in ("test", "x", "n/a") or last.lower() in ("test", "x", "n/a"):
            continue
        title = (basic.get("credential") or "").strip()
        addrs = rec.get("addresses") or []
        practice = next(
            (a for a in addrs if a.get("address_purpose") == "LOCATION"),
            addrs[0] if addrs else {},
        )
        phone = (practice.get("telephone_number") or "").strip()
        taxes = rec.get("taxonomies") or []
        primary_tax = next(
            (t for t in taxes if t.get("primary")),
            taxes[0] if taxes else {},
        )
        out.append({
            "npi": npi,
            "first": first,
            "last": last,
            "title": title,
            "phone": phone,
            "taxonomy": (primary_tax.get("desc") or "").strip(),
        })

    # Cache the full unfiltered list so the same address is reusable
    try:
        import json as _j
        _cache_put(key, _j.dumps(out))
    except Exception:
        pass

    return [p for p in out if p.get("npi") != exclude_npi][:limit]
