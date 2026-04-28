"""State Secretary-of-State business filings — registered-agent emails.

Public business filings list registered agents + principal officers along
with their addresses and (often) email addresses. These are PUBLIC RECORDS.

Implemented:
  - FL  — Sunbiz (search.sunbiz.org)            [emails on detail page]
  - TX  — SOSDirect mirror via OpenCorporates    [officers; rare email]
  - CA  — bizfileonline.sos.ca.gov               [officers; rare email]
  - NY  — apps.dos.ny.gov entity search          [DOS process address]
  - GA  — ecorp.sos.ga.gov                       [officers]
  - NC  — sosnc.gov business search              [registered agent]

For states without a free programmatic email, we still extract officer
names + the registered-agent address, which feed the LinkedIn resolver
and Bing site-search to surface the actual email on the org's own site.
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
    # Even with zero emails, we may still have officer names — return a
    # sentinel record so callers can use officers for site-search.
    if not out and info.get("officers"):
        out.append({
            "email": "",
            "source": "sunbiz-fl",
            "source_url": info.get("source_url", ""),
            "officers": info.get("officers", []),
            "status": info.get("status", ""),
            "is_generic": False,
        })
    return out


# ─── Generic Bing-via-state-domain lookup for non-FL states ───────────
# Falls back to Bing-restricted search of the state's SoS site, parses
# any officer-name spans, and returns them. Used to enrich states where
# we don't yet have a dedicated parser.

_STATE_SOS_DOMAIN = {
    "TX": "comptroller.texas.gov",
    "CA": "bizfileonline.sos.ca.gov",
    "NY": "apps.dos.ny.gov",
    "GA": "ecorp.sos.ga.gov",
    "NC": "sosnc.gov",
    "PA": "file.dos.pa.gov",
    "OH": "businesssearch.ohiosos.gov",
    "IL": "ilsos.gov",
    "VA": "scc.virginia.gov",
    "AZ": "ecorp.azcc.gov",
    "WA": "ccfs.sos.wa.gov",
    "CO": "coloradosos.gov",
}


async def find_sos_officers(org_name: str, state: Optional[str]) -> dict:
    """Bing-search the state's SoS domain for the org and pull officer names.

    Returns: {"officers": [...], "source_url": "...", "source": "sos-<ST>"}
    Empty dict if nothing useful.
    """
    if not state:
        return {}
    st = state.upper()
    domain = _STATE_SOS_DOMAIN.get(st)
    if not domain:
        return {}
    org = _norm(org_name or "")
    if len(org) < 4:
        return {}

    q = f'site:{domain} "{org}"'
    bing_url = f"https://www.bing.com/search?q={urllib.parse.quote(q)}"

    async with httpx.AsyncClient(
        timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
    ) as c:
        try:
            r = await c.get(bing_url)
            if r.status_code != 200:
                return {}
            html = r.text
        except Exception:
            return {}
        # First on-domain hit
        m = re.search(rf'href="(https?://[^"\']*{re.escape(domain)}[^"\']+)"', html)
        if not m:
            return {}
        detail_url = _html.unescape(m.group(1))
        try:
            await asyncio.sleep(0.4)
            r2 = await c.get(detail_url)
            if r2.status_code != 200:
                return {"source_url": detail_url, "source": f"sos-{st.lower()}"}
            page = _html.unescape(r2.text)
        except Exception:
            return {"source_url": detail_url, "source": f"sos-{st.lower()}"}

    officers: list[dict] = []
    # Generic patterns common to most state SoS sites
    for m in re.finditer(
        r"(Manager|Member|Officer|Director|Agent|President|CEO|CFO|Secretary|Treasurer|Owner)\s*[:\-]?\s*</?[^>]*>?\s*([A-Z][A-Z, .'\-]{3,60})",
        page,
    ):
        title, name = m.group(1).strip(), m.group(2).strip()
        if name and len(name) > 3 and len(name) < 60:
            officers.append({"title": title, "name": name})
    # Dedup
    seen, deduped = set(), []
    for o in officers:
        key = (o["title"].lower(), o["name"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(o)
    return {
        "officers": deduped[:6],
        "source_url": detail_url,
        "source": f"sos-{st.lower()}",
    }


async def find_state_filings(org_name: str, state: Optional[str]) -> dict:
    """Unified entry point: returns whatever we can get from the state's SoS.

    Result schema:
        {
          "emails": [...],     # only FL gives these reliably
          "officers": [...],   # any state we have a parser for
          "source": "sunbiz-fl" | "sos-tx" | ...,
          "source_url": "...",
        }
    """
    out: dict = {"emails": [], "officers": [], "source": "", "source_url": ""}
    if not state:
        return out
    st = state.upper()
    if st == "FL":
        hits = await find_sunbiz_emails(org_name=org_name, state=st)
        if hits:
            first = hits[0]
            out["source"] = first.get("source", "sunbiz-fl")
            out["source_url"] = first.get("source_url", "")
            out["officers"] = first.get("officers", []) or []
            out["emails"] = [h.get("email", "") for h in hits if h.get("email")]
        return out
    # Generic SoS Bing-route for other states
    info = await find_sos_officers(org_name=org_name, state=st)
    if info:
        out.update({
            "officers": info.get("officers", []),
            "source": info.get("source", ""),
            "source_url": info.get("source_url", ""),
        })
    return out

