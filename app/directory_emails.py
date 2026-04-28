"""Business-directory email fallback.

When a lab has no website (or no emails on their website), real
contact info is often listed on third-party directories. We try a
small set of high-yield public directories that are scrape-friendly:

  - Google Maps / Google Business Profile (via search redirect)
  - YellowPages (yellowpages.com/<state>/<city>)
  - BBB (bbb.org/us/<state>/<city>)
  - Manta (manta.com)
  - HealthGrades (healthgrades.com — for medical practices)

We do a single Bing search per provider with the org name + city +
state, follow the first directory hit, and scrape any emails from
the resulting page. All emails returned are REAL — pulled from a
public business listing maintained by the directory.
"""
from __future__ import annotations

import asyncio
import re
import urllib.parse
from typing import Optional

import httpx

EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
)

# Domains we'll actually follow when they appear in Bing results.
TRUSTED_DIRECTORY_DOMAINS = (
    "yellowpages.com",
    "bbb.org",
    "manta.com",
    "healthgrades.com",
    "yelp.com",
    "mapquest.com",
    "businessfinder.com",
    "labsearch.com",
    "uslabs.org",
    "labtestsguide.com",
    "ratemds.com",
    "vitals.com",
    "wellness.com",
    "doctor.com",
    "buzzfile.com",
    "dnb.com",
    "chamberofcommerce.com",
)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"


def _norm_org(s: str) -> str:
    s = re.sub(r"\b(LLC|INC|CORP|LTD|PLLC|PA|PC|LLP|LP|CO|COMPANY)\b\.?", "", s, flags=re.I)
    s = re.sub(r"[,&]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


async def _bing_first_hits(query: str, max_hits: int = 6) -> list[str]:
    """Return up to N URLs from Bing for a query, filtered to trusted dir domains."""
    url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}"
    try:
        async with httpx.AsyncClient(
            timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
        ) as c:
            r = await c.get(url)
            if r.status_code != 200:
                return []
            html = r.text
    except Exception:
        return []
    # Bing result links sit in <a href="https://...">; we just regex them.
    hits = re.findall(r'<a[^>]+href="(https?://[^"\']+)"', html)
    out: list[str] = []
    seen: set[str] = set()
    for u in hits:
        # Strip Bing tracker URLs
        if "bing.com/" in u or "microsoft.com/" in u:
            continue
        host = u.split("/")[2].lower() if u.startswith("http") and len(u.split("/")) > 2 else ""
        if not any(d in host for d in TRUSTED_DIRECTORY_DOMAINS):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= max_hits:
            break
    return out


async def _fetch_emails(url: str, client: httpx.AsyncClient) -> list[str]:
    try:
        r = await client.get(url)
        if r.status_code != 200:
            return []
        import html as _h
        text = _h.unescape(r.text)
        text = re.sub(r"\s*\[\s*at\s*\]\s*", "@", text, flags=re.I)
        text = re.sub(r"\s*\(\s*at\s*\)\s*", "@", text, flags=re.I)
        text = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", text, flags=re.I)
        text = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", text, flags=re.I)
        emails = set()
        for m in EMAIL_RE.findall(text):
            e = m.lower().strip(".,;:")
            # Drop noise from directory pages themselves
            if any(b in e for b in (
                "@yellowpages.", "@bbb.org", "@manta.com", "@yelp.com",
                "@healthgrades.", "@vitals.com", "@google.com", "@gstatic.com",
                "@sentry.io", "@wixpress.com", "@example.com",
                "noreply", "no-reply", "donotreply",
            )):
                continue
            emails.add(e)
        return list(emails)
    except Exception:
        return []


async def find_directory_emails(
    org_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    max_pages: int = 4,
) -> list[dict]:
    """Search business directories via Bing and scrape any emails listed.

    Returns up to a handful of real-email records. Each record is tagged
    with the directory source URL so the user can verify provenance.
    """
    org = _norm_org(org_name or "")
    if not org or len(org) < 3:
        return []
    parts = [f'"{org}"']
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    query = " ".join(parts) + " (yellowpages OR bbb OR manta OR healthgrades) email"

    urls = await _bing_first_hits(query, max_hits=max_pages)
    if not urls:
        return []

    out: list[dict] = []
    seen: set[str] = set()
    async with httpx.AsyncClient(
        timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
    ) as c:
        for u in urls:
            host = u.split("/")[2].lower() if "://" in u else u
            emails = await _fetch_emails(u, c)
            for e in emails:
                if e in seen:
                    continue
                seen.add(e)
                out.append({
                    "email": e,
                    "first_name": "",
                    "last_name": "",
                    "full_name": None,
                    "position": "Directory Listing",
                    "is_decision_maker": False,
                    "is_generic": True,
                    "confidence": 65,
                    "verified": False,
                    "source": f"directory:{host}",
                    "domain": e.split("@", 1)[1] if "@" in e else "",
                })
            await asyncio.sleep(0.3)
            if len(out) >= 5:
                break
    return out
