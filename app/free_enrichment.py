"""Free-tier email + LinkedIn enrichment using only public, no-signup APIs.

Sources used (all free, no API key required):
  1. PubMed E-utilities (NIH) — finds researcher emails in paper affiliations
  2. Semantic Scholar API — finds author affiliations + homepages
  3. Bing/DDG search — finds emails mentioned about a lab off-site
  4. Enhanced website scraping — obfuscated emails, all 20 page patterns
  5. WHOIS contact emails (RDAP public endpoint)

Call `enrich_contact(first, last, org, domain)` to get emails + LinkedIn profile.
"""

from __future__ import annotations

import asyncio
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import NamedTuple

import httpx

_FAKE_DOMAINS = frozenset([
    "example.com", "test.com", "placeholder.com", "yourcompany.com",
    "company.com", "website.com", "sentry.io", "wixpress.com",
    "domain.com", "yourdomain.com", "mail.com", "noreply.com",
])
_FAKE_USERS = frozenset([
    "yourname", "firstname", "lastname", "youremail", "name",
    "your", "email", "user", "webmaster", "noreply", "no-reply",
])


class EnrichedContact(NamedTuple):
    email: str
    source: str          # "pubmed", "semantic_scholar", "web_scrape", "offsite_search", "whois"
    confidence: int      # 0-100
    verdict: str         # "deliverable" / "risky" / "unknown"
    linkedin_profile: str


def _clean_email(raw: str) -> str | None:
    """Normalise and validate an extracted email string."""
    email = raw.strip().lower().strip("\"'<>.,;")
    if "@" not in email:
        return None
    user, domain = email.rsplit("@", 1)
    if not user or not domain or "." not in domain:
        return None
    if domain in _FAKE_DOMAINS or user in _FAKE_USERS:
        return None
    if len(email) > 120:
        return None
    return email


_EMAIL_RE = re.compile(
    r"""[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}""", re.ASCII
)
# Obfuscated patterns: "name [at] domain [dot] com" or "name(at)domain.com"
_OBFUSC_RE = re.compile(
    r"""([A-Za-z0-9._%+\-]+)\s*[\(\[]\s*at\s*[\)\]]\s*([A-Za-z0-9.\-]+)\s*[\(\[]\s*dot\s*[\)\]]\s*([A-Za-z]{2,})""",
    re.IGNORECASE,
)
_OBFUSC2_RE = re.compile(
    r"""([A-Za-z0-9._%+\-]+)\s*\[at\]\s*([A-Za-z0-9.\-]+\.[A-Za-z]{2,})""",
    re.IGNORECASE,
)


def _extract_emails(text: str) -> list[str]:
    found = []
    for m in _EMAIL_RE.finditer(text):
        e = _clean_email(m.group())
        if e:
            found.append(e)
    for m in _OBFUSC_RE.finditer(text):
        e = _clean_email(f"{m.group(1)}@{m.group(2)}.{m.group(3)}")
        if e:
            found.append(e)
    for m in _OBFUSC2_RE.finditer(text):
        e = _clean_email(f"{m.group(1)}@{m.group(2)}")
        if e:
            found.append(e)
    return list(dict.fromkeys(found))  # dedup, preserve order


def _extract_linkedin_profiles(text: str) -> list[str]:
    slugs = re.findall(r"linkedin\.com/in/([A-Za-z0-9_\-]+)", text)
    return [f"https://www.linkedin.com/in/{s}" for s in dict.fromkeys(slugs) if len(s) >= 3]


# ─── PubMed E-utilities (NIH, completely free) ────────────────────────────────

def pubmed_search_emails(org: str, first: str = "", last: str = "") -> list[EnrichedContact]:
    """Search PubMed for authors affiliated with `org` and extract their emails."""
    results: list[EnrichedContact] = []
    try:
        # Build query: author name + affiliation
        query_parts = []
        if first and last:
            query_parts.append(f"{last} {first[0]}[Author]")
        if org:
            clean_org = re.sub(r"\b(llc|inc|corp|lab|labs|laboratory|diagnostics|dx)\b", "", org, flags=re.I).strip()
            if clean_org:
                query_parts.append(f'"{clean_org}"[Affiliation]')
        if not query_parts:
            return results

        query = " AND ".join(query_parts)
        search_url = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            f"?db=pubmed&term={urllib.parse.quote(query)}&retmax=5&retmode=json"
        )
        req = urllib.request.Request(
            search_url,
            headers={"User-Agent": "MedPharmaResearch/1.0 (research; contact@medprosc.com)"},
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            import json
            data = json.loads(r.read())
        ids = data.get("esearchresult", {}).get("idlist", [])
        if not ids:
            return results

        # Fetch abstracts for the found articles
        fetch_url = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            f"?db=pubmed&id={','.join(ids[:5])}&retmode=xml"
        )
        req2 = urllib.request.Request(
            fetch_url,
            headers={"User-Agent": "MedPharmaResearch/1.0"},
        )
        with urllib.request.urlopen(req2, timeout=12) as r2:
            xml_bytes = r2.read()

        root = ET.fromstring(xml_bytes)
        for article in root.iter("PubmedArticle"):
            for affil in article.iter("Affiliation"):
                if not affil.text:
                    continue
                emails = _extract_emails(affil.text)
                for email in emails:
                    results.append(EnrichedContact(
                        email=email,
                        source="pubmed",
                        confidence=85,
                        verdict="deliverable",  # PubMed emails are real published addresses
                        linkedin_profile="",
                    ))
    except Exception:
        pass
    return results


# ─── Semantic Scholar API (free, no key) ──────────────────────────────────────

def semantic_scholar_search(org: str, first: str = "", last: str = "") -> list[EnrichedContact]:
    """Search Semantic Scholar for author homepages and affiliations."""
    results: list[EnrichedContact] = []
    try:
        if first and last:
            query = f"{first} {last}"
        elif org:
            query = org
        else:
            return results

        url = (
            "https://api.semanticscholar.org/graph/v1/author/search"
            f"?query={urllib.parse.quote(query)}"
            "&fields=name,affiliations,homepage,externalIds"
            "&limit=5"
        )
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "MedPharmaResearch/1.0"},
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            import json
            data = json.loads(r.read())

        for author in data.get("data", []):
            homepage = author.get("homepage") or ""
            name = author.get("name") or ""
            affils = author.get("affiliations") or []
            affil_names = [a.get("name", "") for a in affils]

            # Check if affiliation matches our org
            org_lower = org.lower() if org else ""
            match = any(org_lower in (a.lower()) for a in affil_names if a) if org_lower else True

            if match and homepage:
                emails = _extract_emails(homepage)
                for email in emails:
                    results.append(EnrichedContact(
                        email=email,
                        source="semantic_scholar",
                        confidence=80,
                        verdict="deliverable",
                        linkedin_profile="",
                    ))
    except Exception:
        pass
    return results


# ─── RDAP / WHOIS contact email ───────────────────────────────────────────────

def rdap_contact_email(domain: str) -> list[EnrichedContact]:
    """Use IANA RDAP (public, no auth) to get domain registrant contact emails."""
    results: list[EnrichedContact] = []
    try:
        url = f"https://rdap.org/domain/{urllib.parse.quote(domain)}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as r:
            import json
            data = json.loads(r.read())

        # Walk vCard arrays for email entries
        def _walk(obj: object) -> None:
            if isinstance(obj, dict):
                for v in obj.values():
                    _walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, str):
                        emails = _extract_emails(item)
                        for email in emails:
                            results.append(EnrichedContact(
                                email=email,
                                source="whois",
                                confidence=55,
                                verdict="risky",
                                linkedin_profile="",
                            ))
                    else:
                        _walk(item)

        _walk(data)
    except Exception:
        pass
    return results


# ─── Off-site Bing search for emails ──────────────────────────────────────────

async def bing_search_emails(org: str, domain: str) -> list[EnrichedContact]:
    """Search Bing for emails/LinkedIn profiles related to a lab off their website."""
    results: list[EnrichedContact] = []
    queries = []
    if domain:
        queries.append(f'"{domain}" email director OR manager OR laboratory')
        queries.append(f'site:{domain} email OR contact')
    if org:
        clean = re.sub(r"\b(llc|inc|corp)\b", "", org, flags=re.I).strip()
        if clean:
            queries.append(f'"{clean}" laboratory director email site:linkedin.com/in')

    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
        ) as client:
            for q in queries[:2]:
                try:
                    resp = await client.get(
                        f"https://www.bing.com/search?q={urllib.parse.quote(q)}&count=10"
                    )
                    if resp.status_code == 200:
                        text = resp.text
                        for email in _extract_emails(text):
                            if domain and domain in email:
                                results.append(EnrichedContact(
                                    email=email,
                                    source="offsite_search",
                                    confidence=70,
                                    verdict="risky",
                                    linkedin_profile="",
                                ))
                        for li in _extract_linkedin_profiles(text):
                            results.append(EnrichedContact(
                                email="",
                                source="offsite_search",
                                confidence=75,
                                verdict="unknown",
                                linkedin_profile=li,
                            ))
                except Exception:
                    pass
    except Exception:
        pass
    return results


# ─── Enhanced website scraper (all 20 pages + obfuscated) ─────────────────────

_SCRAPE_PAGES = [
    "", "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/our-team", "/leadership", "/leadership-team",
    "/staff", "/medical-staff", "/physicians", "/providers",
    "/practitioners", "/directory", "/people", "/meet-the-team",
    "/administration", "/management", "/meet-us",
]


async def scrape_website_emails(domain: str) -> list[EnrichedContact]:
    """Scrape all standard lab website pages for emails (including obfuscated)."""
    results: list[EnrichedContact] = []
    found_emails: set[str] = set()
    found_linkedin: set[str] = set()

    async with httpx.AsyncClient(
        timeout=7.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    ) as client:

        async def _fetch(path: str) -> None:
            for scheme in ("https", "http"):
                try:
                    resp = await client.get(f"{scheme}://{domain}{path}")
                    if resp.status_code == 200:
                        text = resp.text
                        for email in _extract_emails(text):
                            if email not in found_emails:
                                found_emails.add(email)
                                results.append(EnrichedContact(
                                    email=email,
                                    source="web_scrape",
                                    confidence=70,
                                    verdict="risky",
                                    linkedin_profile="",
                                ))
                        for li in _extract_linkedin_profiles(text):
                            if li not in found_linkedin:
                                found_linkedin.add(li)
                                results.append(EnrichedContact(
                                    email="",
                                    source="web_scrape",
                                    confidence=75,
                                    verdict="unknown",
                                    linkedin_profile=li,
                                ))
                        return
                except Exception:
                    continue

        # Fetch all pages concurrently (limit to 10 at a time)
        for i in range(0, len(_SCRAPE_PAGES), 10):
            batch = _SCRAPE_PAGES[i:i + 10]
            await asyncio.gather(*[_fetch(p) for p in batch])

    return results


# ─── Master enrichment function ───────────────────────────────────────────────

def enrich_contact(
    first: str = "",
    last: str = "",
    org: str = "",
    domain: str = "",
) -> dict:
    """Run all free enrichment sources and return deduplicated ranked results.

    Returns:
        {
            "emails": [{"email": str, "source": str, "confidence": int, "verdict": str}],
            "linkedin_profiles": [str],
            "best_email": str | None,
            "best_email_source": str,
            "best_confidence": int,
        }
    """
    contacts: list[EnrichedContact] = []

    # 1. PubMed (sync, fast for known researchers)
    if org or (first and last):
        contacts.extend(pubmed_search_emails(org, first, last))

    # 2. Semantic Scholar
    if first and last:
        contacts.extend(semantic_scholar_search(org, first, last))

    # 3. RDAP/WHOIS
    if domain:
        contacts.extend(rdap_contact_email(domain))

    # 4. Website scraping + Bing off-site (async)
    async def _async_enrich() -> list[EnrichedContact]:
        tasks = []
        if domain:
            tasks.append(scrape_website_emails(domain))
            tasks.append(bing_search_emails(org, domain))
        results_lists = await asyncio.gather(*tasks)
        out = []
        for lst in results_lists:
            out.extend(lst)
        return out

    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
        extra = loop.run_until_complete(_async_enrich())
    except RuntimeError:
        extra = asyncio.run(_async_enrich())
    contacts.extend(extra)

    # Deduplicate + rank
    seen_emails: dict[str, EnrichedContact] = {}
    linkedin_profiles: list[str] = []
    seen_li: set[str] = set()

    SOURCE_PRIORITY = {
        "pubmed": 95,
        "semantic_scholar": 80,
        "web_scrape": 70,
        "offsite_search": 65,
        "whois": 55,
    }

    for c in contacts:
        if c.linkedin_profile and c.linkedin_profile not in seen_li:
            seen_li.add(c.linkedin_profile)
            linkedin_profiles.append(c.linkedin_profile)
        if c.email:
            key = c.email
            if key not in seen_emails:
                seen_emails[key] = c
            else:
                # Keep higher-confidence source
                existing = seen_emails[key]
                if SOURCE_PRIORITY.get(c.source, 0) > SOURCE_PRIORITY.get(existing.source, 0):
                    seen_emails[key] = c

    emails = sorted(
        [
            {
                "email": c.email,
                "source": c.source,
                "confidence": c.confidence,
                "verdict": c.verdict,
            }
            for c in seen_emails.values()
        ],
        key=lambda x: (-x["confidence"], x["email"]),
    )

    best = emails[0] if emails else None
    return {
        "emails": emails,
        "linkedin_profiles": linkedin_profiles,
        "best_email": best["email"] if best else None,
        "best_email_source": best["source"] if best else "",
        "best_confidence": best["confidence"] if best else 0,
    }
