"""
Email enrichment for lab leads — REAL emails only.

Hunter.io endpoints used:
  /v2/domain-search   — all emails at a domain (decision makers first)
  /v2/email-finder    — find a specific person's email by first+last+domain
  /v2/email-verifier  — verify any email address
  /v2/combined/find   — full lead enrichment from an email address
"""

import re
import asyncio
import httpx
from typing import Optional
from app.config import HUNTER_API_KEY



def _org_name_to_domain_candidates(org_name: str) -> list[str]:
    """Derive likely domain candidates from an org name."""
    strip_words = {
        "inc", "llc", "ltd", "corp", "corporation", "co", "company",
        "pllc", "pa", "pc", "dba", "the", "and", "&", "of", "a",
    }
    name = org_name.lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    tokens = [t for t in name.split() if t not in strip_words]

    if not tokens:
        return []

    abbrev_map = {
        "laboratory": "lab", "laboratories": "labs",
        "medical": "med", "diagnostics": "dx", "diagnostic": "dx",
        "pathology": "path", "services": "svc", "associates": "assoc",
        "center": "ctr", "reference": "ref", "clinical": "clinical",
        "health": "health", "healthcare": "healthcare",
    }
    abbrev_tokens = [abbrev_map.get(t, t) for t in tokens]

    candidates = []
    full = "".join(tokens)
    candidates.append(full + ".com")

    abbrev = "".join(abbrev_tokens)
    if abbrev != full:
        candidates.append(abbrev + ".com")

    if tokens[0] not in ("lab", "labs", "clinical", "medical", "med"):
        candidates.append(tokens[0] + "labs.com")
        candidates.append(tokens[0] + "lab.com")
        candidates.append(tokens[0] + "med.com")

    if len(tokens) >= 2:
        candidates.append("".join(tokens[:2]) + ".com")

    candidates.append(tokens[0] + ".com")

    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


async def _check_domain_exists(domain: str, client: httpx.AsyncClient) -> bool:
    """Return True if the domain resolves to a live website."""
    for scheme in ("https", "http"):
        try:
            resp = await client.head(
                f"{scheme}://{domain}", timeout=5.0, follow_redirects=True,
            )
            if resp.status_code < 500:
                return True
        except Exception:
            pass
    return False


async def _find_live_domain(candidates: list[str]) -> Optional[str]:
    """Check all candidates concurrently, return the first live one."""
    async with httpx.AsyncClient(timeout=6.0) as client:
        results = await asyncio.gather(
            *[_check_domain_exists(d, client) for d in candidates],
            return_exceptions=True,
        )
    for domain, result in zip(candidates, results):
        if result is True:
            return domain
    return None


DECISION_MAKER_KEYWORDS = [
    "director", "owner", "president", "ceo", "cfo", "coo", "chief",
    "manager", "administrator", "vp", "vice president", "principal",
    "founder", "partner", "executive",
]


def _build_email_record(email_str: str, first: str, last: str, position: str,
                        confidence: int, verified: bool, source: str, domain: str) -> dict:
    is_dm = any(kw in (position or "").lower() for kw in DECISION_MAKER_KEYWORDS)
    return {
        "email": email_str,
        "first_name": first or "",
        "last_name": last or "",
        "full_name": f"{first} {last}".strip() or None,
        "position": position or "",
        "is_decision_maker": is_dm,
        "confidence": confidence,
        "verified": verified,
        "source": source,
        "domain": domain,
    }


async def hunter_domain_search(domain: str, api_key: str) -> tuple[list[dict], int]:
    """GET /v2/domain-search — all emails at a domain, decision makers first."""
    url = "https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": api_key, "limit": 20}

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return [], 0
            data = resp.json()
        except Exception:
            return [], 0

    decision_makers, others = [], []
    for entry in data.get("data", {}).get("emails", []):
        record = _build_email_record(
            email_str=entry.get("value", ""),
            first=entry.get("first_name") or "",
            last=entry.get("last_name") or "",
            position=entry.get("position") or "",
            confidence=entry.get("confidence", 0),
            verified=(entry.get("verification") or {}).get("status") == "valid",
            source="hunter.io/domain-search",
            domain=domain,
        )
        (decision_makers if record["is_decision_maker"] else others).append(record)

    decision_makers.sort(key=lambda e: -e["confidence"])
    others.sort(key=lambda e: -e["confidence"])
    total = data.get("data", {}).get("meta", {}).get("total", 0)
    return decision_makers + others, total


async def hunter_email_finder(domain: str, first_name: str, last_name: str,
                               api_key: str) -> Optional[dict]:
    """
    GET /v2/email-finder — find a specific person's email by name + domain.
    Returns a single email record or None.
    """
    url = "https://api.hunter.io/v2/email-finder"
    params = {
        "domain": domain,
        "first_name": first_name,
        "last_name": last_name,
        "api_key": api_key,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return None
            data = resp.json()
        except Exception:
            return None

    entry = data.get("data") or {}
    email_str = entry.get("email") or ""
    if not email_str:
        return None

    return _build_email_record(
        email_str=email_str,
        first=entry.get("first_name") or first_name,
        last=entry.get("last_name") or last_name,
        position=entry.get("position") or "",
        confidence=entry.get("score", 0),
        verified=(entry.get("verification") or {}).get("status") == "valid",
        source="hunter.io/email-finder",
        domain=domain,
    )


async def hunter_verify_email(email: str, api_key: str) -> dict:
    """
    GET /v2/email-verifier — verify a single email address.
    Returns {email, status, score, is_valid}.
    """
    url = "https://api.hunter.io/v2/email-verifier"
    params = {"email": email, "api_key": api_key}
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return {"email": email, "is_valid": False, "status": "unknown", "score": 0}
            data = resp.json().get("data", {})
        except Exception:
            return {"email": email, "is_valid": False, "status": "error", "score": 0}

    status = data.get("status", "unknown")
    return {
        "email": email,
        "is_valid": status in ("valid", "accept_all"),
        "status": status,
        "score": data.get("score", 0),
        "mx_records": data.get("mx_records", False),
        "smtp_server": data.get("smtp_server", False),
        "smtp_check": data.get("smtp_check", False),
    }


async def hunter_combined_enrichment(email: str, api_key: str) -> dict:
    """
    GET /v2/combined/find — full person + company enrichment from an email.
    Returns flattened person + company fields.
    """
    url = "https://api.hunter.io/v2/combined/find"
    params = {"email": email, "api_key": api_key}
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return {}
            raw = resp.json().get("data", {})
        except Exception:
            return {}

    person = raw.get("person") or {}
    company = raw.get("company") or {}
    return {
        "email": email,
        "full_name": person.get("name", {}).get("fullName") or "",
        "first_name": person.get("name", {}).get("givenName") or "",
        "last_name": person.get("name", {}).get("familyName") or "",
        "title": person.get("title") or "",
        "linkedin": (person.get("linkedin") or {}).get("handle") or "",
        "twitter": (person.get("twitter") or {}).get("handle") or "",
        "company_name": company.get("name") or "",
        "company_domain": company.get("domain") or "",
        "company_industry": company.get("category", {}).get("industry") or "",
        "company_size": company.get("metrics", {}).get("employeesRange") or "",
        "company_phone": company.get("phone") or "",
    }


async def find_emails_for_lab(
    org_name: str,
    domain_hint: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
) -> dict:
    """
    Main entry point.
    1. Find which domain is actually live
    2. Domain-search: all emails at that domain
    3. Email-finder: targeted lookup if we have a person's name (from NPI data)
    4. Deduplication by email address
    """
    candidates = _org_name_to_domain_candidates(org_name)
    if domain_hint:
        candidates.insert(0, domain_hint)

    live_domain = await _find_live_domain(candidates)

    result: dict = {
        "org_name": org_name,
        "domain_candidates": candidates[:6],
        "live_domain": live_domain,
        "hunter_enabled": bool(HUNTER_API_KEY),
        "emails": [],
        "total_at_domain": 0,
        "error": None,
    }

    if not live_domain:
        result["error"] = "Could not confirm a live website for this organization. Enter the domain manually."
        return result

    if not HUNTER_API_KEY:
        result["error"] = (
            f"Live domain found: {live_domain} — "
            "Add your HUNTER_API_KEY to pull real named emails automatically."
        )
        return result

    # Run domain-search and (optionally) email-finder concurrently
    tasks: list = [hunter_domain_search(live_domain, HUNTER_API_KEY)]
    run_finder = bool(first_name and last_name)
    if run_finder:
        tasks.append(hunter_email_finder(live_domain, first_name, last_name, HUNTER_API_KEY))

    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    domain_emails, total = (task_results[0] if not isinstance(task_results[0], Exception) else ([], 0))
    finder_email = (task_results[1] if run_finder and not isinstance(task_results[1], Exception) else None)

    # Merge, deduplicating by email address; keep finder result first (it's name-specific)
    seen: set = set()
    merged: list = []
    if finder_email and finder_email.get("email"):
        seen.add(finder_email["email"])
        merged.append(finder_email)
    for rec in domain_emails:
        if rec["email"] not in seen:
            seen.add(rec["email"])
            merged.append(rec)

    result["emails"] = merged
    result["total_at_domain"] = total

    if not merged:
        result["error"] = (
            f"Domain {live_domain} is live but Hunter.io found no emails yet. "
            "Try searching it directly at hunter.io."
        )
    return result
