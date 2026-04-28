"""CLIA Provider of Services enrichment.

CMS publishes the full Provider of Services file for clinical labs as
open data (no auth, no rate limits). Per lab we get:
  - FORM_116_ACRDTD_TEST_VOL_CNT: annual accredited test volume
  - FORM_116_TEST_VOL_CNT: non-accredited volume
  - WVD_TEST_VOL_CNT: waived test volume
  - FAX_PHNE_NUM: lab fax (extra contact channel)
  - PHNE_NUM: lab phone
  - CRTFCT_TYPE_CD: certificate type (1=Waived, 3=Compliance, 4=Accredited, 9=PPMP)
  - PGM_TRMNTN_CD: program termination code (00 = active)
  - Accreditation flags: CAP, JCAHO, COLA, A2LA, AOA, AABB, ASHI

Loaded once into memory at startup keyed by ZIP+street prefix for
fast match against NPPES rows.
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Optional

import httpx

DATASET_ID = "d3eb38ac-d8e9-40d3-b7b7-6205d3d1dc16"
DATA_URL = f"https://data.cms.gov/data-api/v1/dataset/{DATASET_ID}/data"

# In-memory index keyed by (state, normalized street prefix).
# Loaded lazily on first call to enrich_with_clia.
_INDEX: dict[tuple[str, str], dict] = {}
_LOADED_AT: float = 0.0
_LOADING_LOCK = asyncio.Lock()
_CACHE_TTL_S = 24 * 3600  # refresh once a day


def _norm_street(s: str) -> str:
    """Normalize street for matching: uppercase, strip suite/unit, first token only."""
    if not s:
        return ""
    up = s.upper()
    # Drop suite/ste/unit/# segments
    up = re.split(r"\b(STE|SUITE|UNIT|APT|FL|FLOOR|#|BLDG|BUILDING)\b", up, maxsplit=1)[0]
    # First 3 tokens of meaningful street ID
    toks = re.findall(r"[A-Z0-9]+", up)
    return " ".join(toks[:3])


async def _load_index(force: bool = False) -> int:
    """Page through the full dataset and index by (state, street_prefix).

    Returns number of rows indexed. Safe to call repeatedly — protected
    by lock + TTL.
    """
    global _LOADED_AT
    async with _LOADING_LOCK:
        if not force and _INDEX and (time.time() - _LOADED_AT) < _CACHE_TTL_S:
            return len(_INDEX)
        _INDEX.clear()
        page_size = 5000
        offset = 0
        total = 0
        async with httpx.AsyncClient(timeout=120.0) as c:
            while True:
                try:
                    r = await c.get(DATA_URL, params={"size": page_size, "offset": offset})
                except Exception:
                    break
                if r.status_code != 200:
                    break
                rows = r.json() if r.content else []
                if not isinstance(rows, list) or not rows:
                    break
                for row in rows:
                    state = (row.get("STATE_CD") or "").strip().upper()
                    street = _norm_street(row.get("ST_ADR") or "")
                    if not state or not street:
                        continue
                    key = (state, street)
                    # Prefer rows that look "active" (PGM_TRMNTN_CD == '00')
                    existing = _INDEX.get(key)
                    if existing and (existing.get("PGM_TRMNTN_CD") or "") == "00":
                        continue
                    _INDEX[key] = row
                    total += 1
                if len(rows) < page_size:
                    break
                offset += page_size
                if offset > 500_000:  # safety brake
                    break
        _LOADED_AT = time.time()
        return total


def _accreditation_summary(row: dict) -> tuple[list[str], int]:
    """Return (list of accrediting bodies present, accreditation count)."""
    flags = []
    pairs = [
        ("CAP", "CAP_ACRDTD_Y_MATCH_SW"),
        ("JCAHO", "JCAHO_ACRDTD_Y_MATCH_SW"),
        ("COLA", "COLA_ACRDTD_Y_MATCH_SW"),
        ("A2LA", "A2LA_ACRDTD_Y_MATCH_SW"),
        ("AOA", "AOA_ACRDTD_Y_MATCH_SW"),
        ("AABB", "AABB_ACRDTD_Y_MATCH_SW"),
        ("ASHI", "ASHI_ACRDTD_Y_MATCH_SW"),
    ]
    for label, col in pairs:
        if (row.get(col) or "").upper() == "Y":
            flags.append(label)
    return flags, len(flags)


def _safe_int(v) -> int:
    try:
        return int(str(v).strip() or 0)
    except (ValueError, TypeError):
        return 0


async def enrich_with_clia(
    state: str,
    street: str,
    zip_code: Optional[str] = None,
) -> dict:
    """Look up CLIA enrichment for a single NPPES address.

    Returns empty dict if no match. Otherwise returns:
        {
            "clia_match": True,
            "clia_number": str,
            "clia_test_volume": int (sum of all volumes),
            "clia_accredited_volume": int,
            "clia_waived_volume": int,
            "clia_active": bool,
            "clia_fax": str,
            "clia_accreditations": list[str],
            "clia_certificate_type": str,
            "clia_facility_name": str,
        }
    """
    await _load_index()
    if not state or not street:
        return {}
    key = (state.strip().upper(), _norm_street(street))
    row = _INDEX.get(key)
    if not row:
        return {}

    accs, _ = _accreditation_summary(row)
    accredited_vol = _safe_int(row.get("FORM_116_ACRDTD_TEST_VOL_CNT"))
    other_vol = _safe_int(row.get("FORM_116_TEST_VOL_CNT"))
    waived_vol = _safe_int(row.get("WVD_TEST_VOL_CNT"))

    cert_type_map = {
        "1": "Certificate of Waiver",
        "2": "Certificate for PPMP",
        "3": "Certificate of Compliance",
        "4": "Certificate of Accreditation",
        "9": "Registration Certificate",
    }
    cert_code = (row.get("CRTFCT_TYPE_CD") or "").strip()

    return {
        "clia_match": True,
        "clia_number": (row.get("PRVDR_NUM") or "").strip(),
        "clia_test_volume": accredited_vol + other_vol + waived_vol,
        "clia_accredited_volume": accredited_vol,
        "clia_waived_volume": waived_vol,
        "clia_active": (row.get("PGM_TRMNTN_CD") or "").strip() == "00",
        "clia_fax": (row.get("FAX_PHNE_NUM") or "").strip(),
        "clia_accreditations": accs,
        "clia_certificate_type": cert_type_map.get(cert_code, cert_code),
        "clia_facility_name": (row.get("FAC_NAME") or "").strip(),
    }


def clia_score_boost(clia: dict) -> int:
    """Compute a heat-score boost from CLIA enrichment.

    Real signals of a high-value lab account:
      - Active CLIA + Certificate of Accreditation: +15
      - High test volume: up to +25 (logarithmic)
      - Each accrediting body (CAP/JCAHO/COLA): +5 each, max +15
    """
    if not clia or not clia.get("clia_match"):
        return 0
    boost = 0
    if clia.get("clia_active"):
        boost += 5
    if clia.get("clia_certificate_type", "").startswith("Certificate of Accreditation"):
        boost += 10
    vol = int(clia.get("clia_test_volume") or 0)
    if vol >= 1_000_000:
        boost += 25
    elif vol >= 250_000:
        boost += 18
    elif vol >= 50_000:
        boost += 12
    elif vol >= 10_000:
        boost += 6
    elif vol >= 1_000:
        boost += 2
    boost += min(15, 5 * len(clia.get("clia_accreditations") or []))
    return boost
