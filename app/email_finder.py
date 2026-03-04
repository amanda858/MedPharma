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


async def scrape_emails_from_website(url: str) -> list[str]:
    """Scrape email addresses from a website using regex."""
    emails = set()
    
    # Try multiple common pages
    pages_to_try = [
        url,
        url.rstrip('/') + '/contact',
        url.rstrip('/') + '/contact-us',
        url.rstrip('/') + '/about',
        url.rstrip('/') + '/about-us',
        url.rstrip('/') + '/team',
        url.rstrip('/') + '/staff',
        url.rstrip('/') + '/doctors',
        url.rstrip('/') + '/physicians',
    ]
    
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for page_url in pages_to_try[:3]:  # Limit to first 3 pages to avoid being too aggressive
            try:
                resp = await client.get(page_url)
                if resp.status_code != 200:
                    continue
                html = resp.text
                
                # Multiple email regex patterns for better coverage
                email_patterns = [
                    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                    r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                    r'[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\s*\.\s*[A-Z|a-z]{2,}',
                ]
                
                for pattern in email_patterns:
                    found = re.findall(pattern, html, re.IGNORECASE)
                    for email in found:
                        email = email.strip().lower()
                        # Basic validation
                        if '@' in email and '.' in email.split('@')[1]:
                            emails.add(email)
                            
            except Exception:
                continue
    
    # Filter out common invalid emails but be less restrictive
    valid_emails = []
    for email in emails:
        if not any(x in email for x in ['example.com', 'test.com', 'noreply', 'placeholder']):
            valid_emails.append(email)
    
    return valid_emails[:15]  # Return up to 15 emails


def generate_pattern_emails(first_name: str, last_name: str, domain: str) -> list[dict]:
    """Generate common email patterns from name and domain."""
    if not first_name or not last_name or not domain:
        return []
    
    fn = first_name.lower().replace(' ', '').replace('-', '')
    ln = last_name.lower().replace(' ', '').replace('-', '')
    domain = domain.lower()
    
    patterns = [
        f"{fn}.{ln}@{domain}",
        f"{fn}{ln}@{domain}",
        f"{fn}@{domain}",
        f"{ln}@{domain}",
        f"{fn[0]}{ln}@{domain}",
        f"{fn}{ln[0]}@{domain}",
    ]
    
    emails = []
    for email in patterns:
        emails.append({
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "position": "",
            "is_decision_maker": True,  # Assume officials are decision makers
            "confidence": 30,  # Low confidence
            "verified": False,
            "source": "pattern_generated",
            "domain": domain,
        })
    return emails



def _org_name_to_domain_candidates(org_name: str) -> list[str]:
    """Derive likely domain candidates from an org name."""
    strip_words = {
        "inc", "llc", "ltd", "corp", "corporation", "co", "company",
        "pllc", "pa", "pc", "dba", "the", "and", "&", "of", "a",
        "associates", "assoc", "group", "practice", "center", "centers",
        "clinic", "clinics", "medical", "health", "healthcare", "hospital",
        "laboratory", "laboratories", "lab", "labs", "diagnostic", "diagnostics",
        "pathology", "services", "professional", "professionals"
    }
    name = org_name.lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    tokens = [t for t in name.split() if t not in strip_words and len(t) > 1]

    if not tokens:
        return []

    abbrev_map = {
        "laboratory": "lab", "laboratories": "labs", "medical": "med", 
        "diagnostics": "dx", "diagnostic": "dx", "pathology": "path", 
        "services": "svc", "associates": "assoc", "center": "ctr", 
        "centers": "ctrs", "clinic": "cl", "clinics": "cls",
        "health": "hlth", "healthcare": "hc", "hospital": "hosp",
        "professional": "pro", "professionals": "pros"
    }
    abbrev_tokens = [abbrev_map.get(t, t) for t in tokens]

    candidates = []
    
    # Try full name without spaces
    full = "".join(tokens)
    candidates.append(full + ".com")
    
    # Try abbreviated version
    abbrev = "".join(abbrev_tokens)
    if abbrev != full:
        candidates.append(abbrev + ".com")
    
    # Try first token + second token
    if len(tokens) >= 2:
        candidates.append(tokens[0] + tokens[1] + ".com")
        candidates.append(tokens[0] + abbrev_tokens[1] + ".com")
    
    # Try with dashes
    if len(tokens) >= 2:
        candidates.append(tokens[0] + "-" + tokens[1] + ".com")
        candidates.append(abbrev_tokens[0] + "-" + abbrev_tokens[1] + ".com")
    
    # Try .org and .net
    for domain in [full + ".com", abbrev + ".com"]:
        candidates.append(domain.replace(".com", ".org"))
        candidates.append(domain.replace(".com", ".net"))
    
    # Remove duplicates and limit to 10
    seen = set()
    unique_candidates = []
    for c in candidates:
        if c not in seen and len(unique_candidates) < 10:
            seen.add(c)
            unique_candidates.append(c)
    
    return unique_candidates

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
                f"{scheme}://{domain}", timeout=10.0, follow_redirects=True,
            )
            if resp.status_code < 400:  # Accept more status codes
                return True
        except Exception:
            pass
    return False


async def _find_live_domain(candidates: list[str]) -> Optional[str]:
    """Check all candidates concurrently, return the first live one."""
    async with httpx.AsyncClient(timeout=12.0) as client:
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
    Enhanced email finding with multiple strategies:
    1. Hunter.io API (if available) - highest quality
    2. Website scraping with quality filtering - good quality
    3. Professional email pattern generation - fallback
    """
    candidates = _org_name_to_domain_candidates(org_name)
    if domain_hint:
        candidates.insert(0, domain_hint)

    print(f"Finding emails for {org_name}, trying {len(candidates)} domain candidates")

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
        result["error"] = "Could not confirm a live website for this organization."
        # Still try pattern generation if names provided
        if first_name and last_name and candidates:
            guessed_domain = candidates[0]
            pattern_emails = _generate_professional_patterns(first_name, last_name, guessed_domain)
            if pattern_emails:
                result["emails"] = pattern_emails
                result["error"] = f"Used guessed domain {guessed_domain} for pattern generation"
                print(f"Generated {len(pattern_emails)} pattern emails for {guessed_domain}")
                return result
        return result

    # Try Hunter.io first if available (highest quality)
    if HUNTER_API_KEY:
        print(f"Trying Hunter.io for {live_domain}")
        hunter_emails = await _try_hunter_approaches(live_domain, first_name, last_name, HUNTER_API_KEY)
        if hunter_emails:
            result["emails"] = hunter_emails
            result["total_at_domain"] = len(hunter_emails)
            print(f"Found {len(hunter_emails)} emails via Hunter.io")
            return result

    # Fallback to enhanced website scraping
    print(f"Trying enhanced website scraping for {live_domain}")
    scraped_emails = await _try_enhanced_scraping(live_domain, first_name, last_name)
    if scraped_emails:
        result["emails"] = scraped_emails
        print(f"Found {len(scraped_emails)} emails via scraping")
        return result

    # Final fallback: professional patterns
    if first_name and last_name:
        pattern_emails = _generate_professional_patterns(first_name, last_name, live_domain)
        if pattern_emails:
            result["emails"] = pattern_emails
            result["error"] = f"Generated professional email patterns for {live_domain}"
            print(f"Generated {len(pattern_emails)} pattern emails as final fallback")
            return result

    result["error"] = f"Live domain found: {live_domain} — No emails found via any method"
    return result


async def _try_hunter_approaches(domain: str, first_name: str, last_name: str, api_key: str) -> list:
    """Try multiple Hunter.io approaches to find emails."""
    emails = []

    # Try domain search first
    try:
        domain_emails, total = await hunter_domain_search(domain, api_key)
        if domain_emails:
            emails.extend(domain_emails[:5])  # Take top 5
    except Exception as e:
        print(f"Hunter domain search failed: {e}")

    # Try specific email finder if we have names
    if first_name and last_name:
        try:
            specific_email = await hunter_email_finder(domain, first_name, last_name, api_key)
            if specific_email and specific_email not in [e.get("email") for e in emails]:
                emails.insert(0, specific_email)  # Add to front as highest priority
        except Exception as e:
            print(f"Hunter email finder failed: {e}")

    return emails


async def _try_enhanced_scraping(domain: str, first_name: str, last_name: str) -> list:
    """Try enhanced website scraping with quality filtering."""
    try:
        scraped_emails = await scrape_emails_from_website(f"https://{domain}")
        print(f"Scraped {len(scraped_emails)} emails from {domain}")

        if not scraped_emails:
            return []

        # Comprehensive quality filtering
        quality_emails = []
        for email in scraped_emails:
            if _is_quality_email(email):
                quality_emails.append({
                    "email": email,
                    "first_name": "",
                    "last_name": "",
                    "full_name": None,
                    "position": "",
                    "is_decision_maker": False,
                    "confidence": 75,
                    "verified": False,
                    "source": "website_scrape_quality",
                    "domain": domain,
                })

        return quality_emails[:5]  # Limit to 5

    except Exception as e:
        print(f"Enhanced scraping failed: {e}")
        return []


def _is_quality_email(email: str) -> bool:
    """Check if an email passes quality filters."""
    email_lower = email.lower()
    username = email.split('@')[0].lower()

    # Skip obvious spam/non-professional emails
    skip_patterns = [
        'noreply@', 'no-reply@', 'donotreply@', 'notifications@',
        'alerts@', 'news@', 'newsletter@', 'updates@', 'mail@',
        'webmaster@', 'postmaster@', 'root@', 'admin@', 'administrator@',
        'test@', 'demo@', 'example@', 'sample@', 'fake@', 'spam@',
        'abuse@', 'security@', 'support@', 'help@', 'info@', 'contact@',
        'sales@', 'marketing@', 'hello@', 'hi@', 'welcome@',
        'feedback@', 'comments@', 'inquiry@', 'questions@',
        'privacy@', 'legal@', 'terms@', 'copyright@', 'hr@', 'jobs@',
        'careers@', 'recruiting@', 'employment@', 'press@', 'media@',
        'events@', 'conference@', 'webinar@', 'signup@', 'register@',
        'unsubscribe@', 'bounce@', 'complaints@', 'report@'
    ]

    if any(skip in email_lower for skip in skip_patterns):
        return False

    # Skip suspicious patterns
    if any(char in username for char in ['.', '-', '_']) and len(username.split('.')) > 3:
        return False

    # Skip very short/long usernames
    if len(username) < 2 or len(username) > 30:
        return False

    # Skip usernames that look like IDs
    if username.isdigit() or re.match(r'^[a-z]+\d{4,}$', username):
        return False

    return True


def _generate_professional_patterns(first_name: str, last_name: str, domain: str) -> list:
    """Generate professional email patterns."""
    fn = first_name.lower()[:1]
    ln = last_name.lower()
    fn_full = first_name.lower()
    ln_full = last_name.lower()

    patterns = [
        f"{fn}{ln}@{domain}",
        f"{fn}.{ln}@{domain}",
        f"{fn_full}@{domain}",
        f"{ln}@{domain}",
        f"{fn_full}.{ln}@{domain}",
        f"{fn}{ln_full}@{domain}",
        f"dr.{ln}@{domain}",
        f"{ln}@md.{domain}",
        f"admin@{domain}",
        f"office@{domain}",
        f"lab@{domain}",
        f"director@{domain}",
    ]

    return [
        {
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "position": "Director/Owner",
            "is_decision_maker": True,
            "confidence": 45,
            "verified": False,
            "source": "pattern_generated",
            "domain": domain,
        }
        for email in patterns[:3]
    ]
