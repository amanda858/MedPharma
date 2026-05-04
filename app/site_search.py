"""Site-restricted Bing search to find email-bearing pages on a target domain.

When direct path crawling (/contact, /about, etc.) on the org's own domain
yields no emails, we ask Bing to find pages on that domain that mention
"email" or "@<domain>" or contact-style terms. We then fetch the top hits
and run our standard email extractor.

This is the single biggest free-tier coverage lift: many real labs only
surface contact info on a deeply-nested page (e.g. /providers/dr-smith,
/locations/orlando, /careers, /press) that we'd never guess by URL.
"""
from __future__ import annotations

import asyncio
import html as _html
import re
import urllib.parse

import httpx

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


async def _search_links(query: str, max_hits: int = 6, domain_filter: str = "") -> list[str]:
    """Fetch search results from DDG (primary) → Bing (fallback).

    DDG does not CAPTCHA from cloud/datacenter IPs; Bing does.
    Optionally filters to URLs whose hostname contains ``domain_filter``.
    """
    import urllib.parse as _up
    search_urls = [
        f"https://html.duckduckgo.com/html/?q={_up.quote(query)}",
        f"https://www.bing.com/search?q={_up.quote(query)}&count=10",
    ]
    for search_url in search_urls:
        try:
            async with httpx.AsyncClient(
                timeout=10.0, follow_redirects=True, headers={"User-Agent": UA}
            ) as c:
                r = await c.get(search_url)
                if r.status_code != 200:
                    continue
                page = r.text
        except Exception:
            continue

        candidates: list[str] = []
        for m in re.findall(r'uddg=([^"&]+)', page):
            u = _up.unquote(m)
            if u.startswith("http"):
                candidates.append(u)
        for u in re.findall(r'<a[^>]+href="(https?://[^"\']+)"', page):
            if "duckduckgo.com/" not in u and "bing.com/" not in u and "microsoft.com/" not in u:
                candidates.append(u)

        out: list[str] = []
        seen: set[str] = set()
        for u in candidates:
            host = u.split("/")[2].lower() if "://" in u else ""
            if domain_filter and domain_filter not in host:
                continue
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
            if len(out) >= max_hits:
                break

        if out:
            return out
    return []


async def _bing_links(query: str, max_hits: int = 6) -> list[str]:
    """Alias kept for compatibility — now uses DDG→Bing chain."""
    return await _search_links(query, max_hits=max_hits)


def _scrape_emails_from_text(html_text: str, target_domain: str) -> list[str]:
    text = _html.unescape(html_text)
    text = re.sub(r"\s*\[\s*at\s*\]\s*", "@", text, flags=re.I)
    text = re.sub(r"\s*\(\s*at\s*\)\s*", "@", text, flags=re.I)
    text = re.sub(r"\s*\{\s*at\s*\}\s*", "@", text, flags=re.I)
    text = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", text, flags=re.I)
    text = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", text, flags=re.I)
    out: set[str] = set()
    for m in EMAIL_RE.findall(text):
        e = m.lower().strip(".,;:")
        if any(b in e for b in ("noreply", "no-reply", "donotreply", "@sentry.io",
                                "@example.com", "@wixpress.com", "@gstatic.com",
                                "@cloudflare.com", "@google.com", "@github.com")):
            continue
        out.add(e)
    # Prefer emails on the target domain
    on_domain = [e for e in out if e.endswith("@" + target_domain)]
    return on_domain if on_domain else list(out)


async def find_emails_via_site_search(
    domain: str,
    org_name: str = "",
    max_pages: int = 5,
) -> list[dict]:
    """Use Bing's `site:<domain>` to surface email-bearing pages, then scrape them.

    Returns records like:
        {"email": "...", "source": "bing-site:<domain>:<url>", "confidence": 75}
    """
    if not domain:
        return []
    domain = domain.lower().strip().lstrip("www.")
    queries = [
        f'site:{domain} ("@{domain}" OR email OR contact)',
        f'site:{domain} (director OR CEO OR owner OR manager) email',
    ]

    candidate_urls: list[str] = []
    seen: set[str] = set()
    for q in queries:
        for u in await _search_links(q, max_hits=max_pages, domain_filter=domain):
            if u in seen:
                continue
            seen.add(u)
            candidate_urls.append(u)
        if len(candidate_urls) >= max_pages:
            break

    if not candidate_urls:
        return []

    out: list[dict] = []
    found_emails: set[str] = set()
    async with httpx.AsyncClient(
        timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
    ) as c:
        for u in candidate_urls[:max_pages]:
            try:
                r = await c.get(u)
                if r.status_code != 200:
                    continue
                emails = _scrape_emails_from_text(r.text, domain)
                for e in emails:
                    if e in found_emails:
                        continue
                    found_emails.add(e)
                    on_domain = e.endswith("@" + domain)
                    out.append({
                        "email": e,
                        "source": f"bing-site:{domain}",
                        "source_url": u,
                        "confidence": 80 if on_domain else 60,
                        "is_generic": e.split("@", 1)[0] in (
                            "info", "contact", "hello", "support", "office",
                            "admin", "inquiry", "inquiries", "billing",
                        ),
                        "is_on_domain": on_domain,
                    })
                if len(out) >= 6:
                    break
            except Exception:
                continue
            await asyncio.sleep(0.3)
    return out
