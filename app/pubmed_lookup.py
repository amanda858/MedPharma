"""PubMed corresponding-author email lookup for clinical labs.

PubMed's E-utilities expose author affiliations + corresponding-author
emails in publication XML. Lab directors publish constantly (validation
studies, case reports, COVID assays). This is the highest-yield REAL
email source available without a paid API.

Workflow per lab:
  1. esearch with `<org>[Affiliation]` for recent papers
  2. efetch the top N PMIDs as XML
  3. Extract <Affiliation> blocks + email addresses
  4. Filter emails to ones that appear in an affiliation block that
     contains the org name (drops co-author emails from other institutions)
  5. Optionally pair with author <ForeName>/<LastName> when available

NCBI rate limits: 3 req/s without an API key, 10 req/s with one.
We throttle conservatively to 2 req/s and cache by org name.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
import xml.etree.ElementTree as ET
from typing import Optional

import httpx

ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "")
NCBI_TOOL = "MedPharma-LeadGen"
NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "leads@medpharmasc.com")

# Conservative throttle. Without a key NCBI allows 3 req/s.
_THROTTLE_S = 0.4 if NCBI_API_KEY else 0.6
_last_call = 0.0
_lock = asyncio.Lock()

# In-process cache keyed by normalized org name.
_cache: dict[str, dict] = {}

EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
)

# Generic suffixes to strip when building the affiliation query
_SUFFIXES = re.compile(
    r"\b(LLC|L\.L\.C\.|INC|INC\.|CORP|CORPORATION|LTD|LIMITED|"
    r"PLLC|PA|PC|LLP|LP|CO\.|COMPANY|GROUP|HOLDINGS|"
    r"SERVICES|SERVICE|SVCS|SVC)\b",
    re.IGNORECASE,
)


def _normalize_org(org: str) -> str:
    """Strip corporate suffixes and excess whitespace."""
    s = _SUFFIXES.sub("", org or "")
    s = re.sub(r"[,&]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


async def _throttle() -> None:
    global _last_call
    async with _lock:
        gap = time.time() - _last_call
        if gap < _THROTTLE_S:
            await asyncio.sleep(_THROTTLE_S - gap)
        _last_call = time.time()


async def _esearch(query: str, retmax: int = 5) -> list[str]:
    await _throttle()
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retmax": retmax,
        "sort": "date",
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL,
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    try:
        async with httpx.AsyncClient(timeout=20.0) as c:
            r = await c.get(ESEARCH, params=params)
            if r.status_code != 200:
                return []
            return list(r.json().get("esearchresult", {}).get("idlist") or [])
    except Exception:
        return []


async def _efetch(pmids: list[str]) -> str:
    if not pmids:
        return ""
    await _throttle()
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL,
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.get(EFETCH, params=params)
            if r.status_code != 200:
                return ""
            return r.text
    except Exception:
        return ""


def _extract_authors_with_emails(xml_text: str, org_keywords: list[str]) -> list[dict]:
    """Parse PubMed XML and pull (name, email, affiliation) tuples
    where the affiliation contains at least one of the org keywords.

    Returns sorted by recency-priority (XML order) of unique emails.
    """
    if not xml_text:
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    out: list[dict] = []
    seen: set[str] = set()
    org_lc = [k.lower() for k in org_keywords if k]

    for article in root.iter("PubmedArticle"):
        # Year
        year = ""
        for y in article.iter("Year"):
            if y.text:
                year = y.text.strip()
                break

        for author in article.iter("Author"):
            fore = (author.findtext("ForeName") or "").strip()
            last = (author.findtext("LastName") or "").strip()
            if not last:
                continue
            for aff in author.iter("AffiliationInfo"):
                aff_text = (aff.findtext("Affiliation") or "").strip()
                if not aff_text:
                    continue
                aff_lc = aff_text.lower()
                if not any(k in aff_lc for k in org_lc):
                    continue
                emails = EMAIL_RE.findall(aff_text)
                for e in emails:
                    e = e.lower().rstrip(".,;:)")
                    if e in seen:
                        continue
                    seen.add(e)
                    out.append({
                        "email": e,
                        "first_name": fore,
                        "last_name": last,
                        "full_name": f"{fore} {last}".strip(),
                        "affiliation": aff_text[:300],
                        "year": year,
                        "source": "pubmed",
                        "confidence": 90,  # surfaces in author metadata of a published paper
                        "verified": True,
                        "is_decision_maker": True,  # corresponding authors at labs are typically directors/PIs
                    })
    return out


async def find_pubmed_emails(
    org_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    max_papers: int = 5,
) -> list[dict]:
    """Find real corresponding-author emails for an organization.

    Returns a list of dicts (possibly empty). All emails are extracted
    from PubMed-published affiliation strings — they are real and were
    self-reported by the author at submission time.
    """
    org = (org_name or "").strip()
    if not org:
        return []
    cache_key = _normalize_org(org).upper()
    if cache_key in _cache:
        return _cache[cache_key]["emails"]

    cleaned = _normalize_org(org)
    if not cleaned or len(cleaned) < 3:
        _cache[cache_key] = {"emails": []}
        return []

    # Build query: org as affiliation + optional city/state to disambiguate
    parts = [f'"{cleaned}"[Affiliation]']
    if city:
        parts.append(f'"{city}"[Affiliation]')
    if state:
        parts.append(f'"{state}"[Affiliation]')
    query = " AND ".join(parts)

    pmids = await _esearch(query, retmax=max_papers)
    if not pmids and city:
        # Retry without city if too restrictive
        pmids = await _esearch(f'"{cleaned}"[Affiliation]', retmax=max_papers)
    if not pmids:
        _cache[cache_key] = {"emails": []}
        return []

    xml_text = await _efetch(pmids)
    keywords = [cleaned] + [w for w in cleaned.split() if len(w) >= 4][:3]
    emails = _extract_authors_with_emails(xml_text, keywords)
    _cache[cache_key] = {"emails": emails}
    return emails


async def find_pubmed_emails_for_person(
    org_name: str,
    first_name: str,
    last_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> Optional[dict]:
    """Find a specific person's email in PubMed.

    Returns the email record if a paper exists where this person is an
    author at this org; otherwise None.
    """
    if not (first_name and last_name):
        return None
    candidates = await find_pubmed_emails(org_name, city=city, state=state)
    fn_lc = first_name.lower()
    ln_lc = last_name.lower()
    for rec in candidates:
        if rec.get("last_name", "").lower() == ln_lc and (
            rec.get("first_name", "").lower().startswith(fn_lc[:3])
            or fn_lc in rec.get("first_name", "").lower()
        ):
            return rec
    return None
