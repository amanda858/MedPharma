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

CACHE_DB = os.environ.get("BACKUP_PEOPLE_CACHE", "/tmp/backup_people_cache_v2.db")
CACHE_TTL_SEC = 24 * 3600

NPI_API = "https://npiregistry.cms.hhs.gov/api/"

# Taxonomies that are STRONG signals this person actually works at a clinical
# lab / pathology practice / medical group running diagnostics. Higher score
# = more relevant backup contact for selling lab billing services.
_LAB_RELEVANT = {
    # tier 1 — lab insiders
    "pathology": 100, "laboratory": 100, "clinical laboratory": 100,
    "medical genetics": 90, "clinical genetics": 90, "cytopathology": 90,
    "hematology": 85, "medical microbiology": 85, "molecular": 85,
    "toxicology": 80,
    # tier 2 — referring physicians who run/own labs
    "internal medicine": 60, "family medicine": 55, "oncology": 70,
    "infectious disease": 65, "endocrinology": 60, "urology": 55,
    "obstetrics": 50, "gynecology": 50, "primary care": 55,
    "nephrology": 60, "rheumatology": 60, "gastroenterology": 60,
    "physician": 50, "medical doctor": 50,
    # tier 3 — mid-level providers in lab-adjacent practices
    "nurse practitioner": 30, "physician assistant": 30,
    "specialist": 25,
}
# Hard-skip these — totally irrelevant for clinical lab outreach
_SKIP_TAXONOMIES = {
    "behavior", "behavioral", "applied behavior", "aba",
    "physical therap", "occupational therap", "speech",
    "chiropract", "acupunctur", "massage",
    "doula", "midwife",
    "counselor", "social worker", "psycholog",
    "dent", "orthodont", "dental hyg",
    "optometr", "audiolog",
    "pharmacist", "pharmacy",
    "dietitian", "nutritionist",
    "podiatr",
    "music therap", "art therap",
    "home health aide", "personal care",
}
# Hard-skip these credentials too
_SKIP_CREDENTIALS = {"dmd", "dds", "dc", "d.c.", "od", "l.ac.", "lac", "lmt",
                     "slp", "ccc-slp", "otd", "otr", "pt", "dpt"}


def _relevance_score(taxonomy: str, credential: str, same_street: bool) -> int:
    tax_l = (taxonomy or "").lower()
    cred_l = (credential or "").lower().strip(".").strip()
    # Hard skip
    for bad in _SKIP_TAXONOMIES:
        if bad in tax_l:
            return -1
    if cred_l in _SKIP_CREDENTIALS:
        return -1
    score = 0
    for kw, pts in _LAB_RELEVANT.items():
        if kw in tax_l:
            score = max(score, pts)
    # Bump MD / DO / PhD
    if cred_l in ("md", "m.d.", "do", "d.o.", "phd", "ph.d."):
        score += 20
    if same_street:
        score += 200  # same street trumps everything
    return score


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


def _norm_street(s: str) -> str:
    """Normalize a street address for fuzzy matching."""
    s = (s or "").upper().strip()
    # Drop suite/unit/apt suffixes
    for sep in (" SUITE ", " STE ", " #", " UNIT ", " APT ", " FL ", " FLOOR "):
        if sep in s:
            s = s.split(sep)[0]
    # Strip trailing punctuation
    return s.rstrip(",. ").strip()


async def find_backup_people(
    zip_code: str = "",
    city: str = "",
    state: str = "",
    street_address: str = "",
    exclude_npi: str = "",
    limit: int = 5,
) -> list[dict]:
    """Return up to ``limit`` individual practitioners (NPI-1) at the same address.

    Ranked: same street address > lab-relevant taxonomy > everything else.
    Drops obvious non-lab taxonomies (PT, Behavior Tech, Chiropractor, etc.)
    """
    if not (zip_code or (city and state)):
        return []
    target_street = _norm_street(street_address)
    key = f"{(zip_code or '').strip()}|{(city or '').strip().upper()}|{(state or '').strip().upper()}"
    cached = _cache_get(key)
    if cached is not None:
        try:
            import json as _j
            cached_list = _j.loads(cached)
            return _rank_and_filter(cached_list, target_street, exclude_npi, limit)
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
        street = (practice.get("address_1") or "").strip()
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
            "street": street,
            "taxonomy": (primary_tax.get("desc") or "").strip(),
        })

    # Cache the full unfiltered list so the same address is reusable
    try:
        import json as _j
        _cache_put(key, _j.dumps(out))
    except Exception:
        pass

    return _rank_and_filter(out, target_street, exclude_npi, limit)


def _rank_and_filter(
    candidates: list[dict],
    target_street: str,
    exclude_npi: str,
    limit: int,
) -> list[dict]:
    scored: list[tuple[int, dict]] = []
    for p in candidates:
        if p.get("npi") == exclude_npi:
            continue
        cand_street = _norm_street(p.get("street", ""))
        same_street = bool(target_street and cand_street and
                           (target_street == cand_street or
                            target_street in cand_street or
                            cand_street in target_street))
        score = _relevance_score(p.get("taxonomy", ""), p.get("title", ""),
                                  same_street)
        if score < 0:
            continue  # hard-skipped
        # Require either same street OR a lab-relevant taxonomy. We will
        # NOT return a random PT in the same ZIP just because they share
        # a postal code — that's noise, not a backup person.
        if score == 0:
            continue
        scored.append((score, p))
    scored.sort(key=lambda x: -x[0])
    return [p for _s, p in scored[:limit]]
