"""State Secretary-of-State business filings — registered-agent emails.

Most labs are LLCs registered with their state's SoS. The filings list a
registered agent + principal officers along with their mailing addresses
and (often) email addresses. These are PUBLIC RECORDS — no scraping risk.

Currently implemented:
  - Florida Sunbiz (search.sunbiz.org) — REAL email field on filings.

Adding more states = same pattern: search → entity detail page → parse.
The state-specific HTML schemas differ, so each state needs its own parser.
"""
from __future__ import annotations

import asyncio
import html as _html
import re
import urllib.parse
from typing import Optional

import httpx

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def _norm(s: str) -> str:
    s = re.sub(r"\b(LLC|INC|CORP|LTD|PLLC|PA|PC|LLP|LP|CO|COMPANY)\b\.?", "", s, flags=re.I)
    s = re.sub(r"[,&]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


async def _sunbiz_search_first(client: httpx.AsyncClient, org: str) -> Optional[str]:
    """Return the first matching entity-detail URL on Sunbiz, or None."""
    q = urllib.parse.quote(org)
    url = (
        "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?"
        f"inquiryType=EntityName&searchTerm={q}&aggregateId="
    )
    try:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        html = r.text
    except Exception:
        return None
    # Result rows link to detail pages: SearchResultDetail?inquirytype=...&directionType=...&searchNameOrder=...&aggregateId=...
    m = re.search(
        r'href="(/Inquiry/CorporationSearch/SearchResultDetail\?[^"]+)"',
        html,
    )
    if not m:
        return None
    return "https://search.sunbiz.org" + _html.unescape(m.group(1))


async def _sunbiz_parse_detail(client: httpx.AsyncClient, url: str) -> dict:
    """Fetch a Sunbiz detail page and pull emails + officer names + status."""
    out: dict = {"source_url": url, "emails": [], "officers": [], "status": ""}
    try:
        r = await client.get(url)
        if r.status_code != 200:
            return out
        html = _html.unescape(r.text)
    except Exception:
        return out

    # Status: ACTIVE / INACTIVE
    m = re.search(r"Status</label>\s*<span[^>]*>([^<]+)</span>", html, re.I)
    if m:
        out["status"] = m.group(1).strip()

    # Emails (anywhere on the page — may be the registered agent's)
    out["emails"] = list({e.lower() for e in EMAIL_RE.findall(html)
                          if not any(b in e.lower() for b in
                          ("@dos.myflorida.com", "@sunbiz.org", "noreply", "example.com"))})

    # Officer/director names — Sunbiz lists them as "Title XX" then "Name".
    # Extract pairs from the officers/directors table.
    for m in re.finditer(
        r"Title\s+([A-Z]{2,})\s*</div>\s*<div[^>]*>\s*([A-Z][A-Z, .'\-]+?)\s*</div>",
        html,
    ):
        title, name = m.group(1).strip(), m.group(2).strip()
        if name and len(name) > 3:
            out["officers"].append({"title": title, "name": name})

    return out


async def find_sunbiz_emails(
    org_name: str,
    state: Optional[str] = None,
) -> list[dict]:
    """Look up an entity on FL Sunbiz and return any emails / officers found.

    Only runs when state == 'FL' (or empty). Other states will route here
    later as we add their parsers.
    """
    if state and state.upper() != "FL":
        return []
    org = _norm(org_name or "")
    if len(org) < 4:
        return []

    async with httpx.AsyncClient(
        timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
    ) as c:
        detail_url = await _sunbiz_search_first(c, org)
        if not detail_url:
            return []
        await asyncio.sleep(0.4)
        info = await _sunbiz_parse_detail(c, detail_url)

    out: list[dict] = []
    for e in info.get("emails", []):
        out.append({
            "email": e,
            "source": "sunbiz-fl",
            "source_url": info.get("source_url", ""),
            "confidence": 75,
            "is_generic": e.split("@", 1)[0] in (
                "info", "contact", "hello", "support", "admin", "office",
            ),
            "officers": info.get("officers", []),
            "status": info.get("status", ""),
        })
    return out
