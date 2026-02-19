"""
Email enrichment for lab leads.

Strategy (no API key required for basic use):
  1. Guess the domain from the organization name
  2. Generate common owner/director email patterns for that domain
  3. If HUNTER_API_KEY is set, verify & discover real emails via Hunter.io

Hunter.io free tier: 25 searches/month — set HUNTER_API_KEY env var to enable.
"""

import re
import os
import httpx
from typing import Optional
from app.config import HUNTER_API_KEY


# Common titles for lab owners / directors we target
DIRECTOR_PREFIXES = [
    "director",
    "labdirector",
    "lab.director",
    "owner",
    "admin",
    "administrator",
    "info",
    "contact",
    "billing",
    "compliance",
    "cfo",
    "ceo",
    "manager",
    "labmanager",
    "lab.manager",
]


def _org_name_to_domain_candidates(org_name: str) -> list[str]:
    """
    Convert an organization name into a list of likely domain candidates.
    E.g. 'Acme Clinical Laboratories Inc' → ['acmeclinicallaboratories.com',
                                               'acmeclinicallab.com',
                                               'acmelabs.com', 'acmelab.com']
    """
    # Strip common suffixes that don't belong in domains
    strip_words = {
        "inc", "llc", "ltd", "corp", "corporation", "co", "company",
        "pllc", "pa", "pc", "dba", "the", "and", "&",
    }

    # Normalise
    name = org_name.lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    tokens = [t for t in name.split() if t not in strip_words]

    if not tokens:
        return []

    candidates = []

    # Full name (all tokens joined)
    full = "".join(tokens)
    candidates.append(f"{full}.com")

    # Replace common long words with abbreviated versions
    abbrev_map = {
        "laboratory": "lab",
        "laboratories": "labs",
        "clinical": "clinical",
        "medical": "med",
        "diagnostics": "dx",
        "pathology": "path",
        "services": "svc",
        "associates": "assoc",
        "center": "ctr",
        "health": "health",
        "reference": "ref",
    }

    abbrev_tokens = [abbrev_map.get(t, t) for t in tokens]
    abbrev = "".join(abbrev_tokens)
    if abbrev != full:
        candidates.append(f"{abbrev}.com")

    # First token + "labs"
    if tokens[0] not in ("lab", "labs", "clinical", "medical"):
        candidates.append(f"{tokens[0]}labs.com")
        candidates.append(f"{tokens[0]}lab.com")

    # First two tokens
    if len(tokens) >= 2:
        candidates.append(f"{''.join(tokens[:2])}.com")

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


def generate_email_patterns(org_name: str) -> list[dict]:
    """
    Generate likely email addresses for lab owners/directors without any API.
    Returns a list of dicts: {email, type, confidence}.
    """
    domains = _org_name_to_domain_candidates(org_name)
    if not domains:
        return []

    # Use the most likely domain (first candidate)
    primary_domain = domains[0]

    emails = []
    for prefix in DIRECTOR_PREFIXES:
        emails.append({
            "email": f"{prefix}@{primary_domain}",
            "type": "pattern",
            "confidence": "low",
            "source": "generated",
            "domain": primary_domain,
        })

    # Also add patterns for alternate domains (info@ and billing@ only)
    for domain in domains[1:3]:
        for prefix in ["info", "billing", "director"]:
            emails.append({
                "email": f"{prefix}@{domain}",
                "type": "pattern",
                "confidence": "low",
                "source": "generated",
                "domain": domain,
            })

    return emails


async def hunter_domain_search(domain: str, api_key: str) -> list[dict]:
    """
    Search Hunter.io for real, verified emails at a domain.
    Returns enriched email records with name, position, and confidence.
    """
    url = "https://api.hunter.io/v2/domain-search"
    params = {
        "domain": domain,
        "api_key": api_key,
        "limit": 10,
        "type": "personal",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return []
        data = resp.json()

    emails = []
    for entry in data.get("data", {}).get("emails", []):
        first = entry.get("first_name", "")
        last = entry.get("last_name", "")
        position = entry.get("position", "")
        confidence = entry.get("confidence", 0)

        # Filter for owner / director level roles
        role_keywords = [
            "director", "owner", "president", "ceo", "cfo", "chief",
            "manager", "administrator", "vp", "vice president", "principal",
        ]
        role_str = position.lower() if position else ""
        is_decision_maker = any(kw in role_str for kw in role_keywords)

        emails.append({
            "email": entry.get("value", ""),
            "first_name": first,
            "last_name": last,
            "position": position,
            "is_decision_maker": is_decision_maker,
            "confidence": confidence,
            "type": "verified" if entry.get("verification", {}).get("status") == "valid" else "unverified",
            "source": "hunter.io",
            "domain": domain,
        })

    # Sort: decision makers first, then by confidence desc
    emails.sort(key=lambda e: (not e["is_decision_maker"], -e["confidence"]))
    return emails


async def find_emails_for_lab(org_name: str, domain_hint: Optional[str] = None) -> dict:
    """
    Main entry point. Returns found emails for a lab organisation.

    If HUNTER_API_KEY is set, tries Hunter.io first.
    Always falls back to generated patterns.
    """
    domains = _org_name_to_domain_candidates(org_name)
    if domain_hint:
        domains.insert(0, domain_hint)

    verified = []
    patterns = generate_email_patterns(org_name)

    if HUNTER_API_KEY and domains:
        # Try the top 2 domain candidates via Hunter.io
        for domain in domains[:2]:
            results = await hunter_domain_search(domain, HUNTER_API_KEY)
            verified.extend(results)
            if verified:
                break  # Found real emails, no need to try more domains

    return {
        "org_name": org_name,
        "domain_candidates": domains[:4],
        "verified_emails": verified,             # From Hunter.io (if key set)
        "pattern_emails": patterns[:12],         # Generated patterns
        "hunter_enabled": bool(HUNTER_API_KEY),
        "total_found": len(verified) + len(patterns[:12]),
    }
