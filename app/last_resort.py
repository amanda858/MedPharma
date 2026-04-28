"""Last-resort accuracy boosters.

Three free, public, no-key sources:

1. Wayback Machine — fetches the most recent archived snapshot of a
   domain when the live site is down. ~30% of "dead" domains have a
   recent snapshot with a working contact page.

2. Officer-targeted site-search — when we have a person's name from
   NPPES or SoS, ask Bing to find pages on the org's domain that
   mention that person. Usually surfaces the staff bio with their
   personal email.

3. Verified LinkedIn URL — Bing for `"<First> <Last>" "<org>"
   site:linkedin.com/in` and parse the first hit's URL into a real
   profile slug (instead of a search URL).
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


def _clean_emails(html_text: str, target_domain: str = "") -> list[str]:
    text = _html.unescape(html_text)
    text = re.sub(r"\s*\[\s*at\s*\]\s*", "@", text, flags=re.I)
    text = re.sub(r"\s*\(\s*at\s*\)\s*", "@", text, flags=re.I)
    text = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", text, flags=re.I)
    text = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", text, flags=re.I)
    out: set[str] = set()
    for m in EMAIL_RE.findall(text):
        e = m.lower().strip(".,;:")
        if any(b in e for b in ("noreply", "no-reply", "donotreply",
                                "@sentry.io", "@example.com", "@wixpress.com",
                                "@gstatic.com", "@cloudflare.com",
                                "@google.com", "@github.com",
                                "@archive.org", "@web.archive.org")):
            continue
        out.add(e)
    if target_domain:
        on = [e for e in out if e.endswith("@" + target_domain)]
        if on:
            return on
    return list(out)


# ─── 1. Wayback Machine ───────────────────────────────────────────────
async def find_wayback_emails(domain: str, max_pages: int = 4) -> list[dict]:
    """Try the most recent Wayback snapshot of <domain>/contact|about|home."""
    if not domain:
        return []
    domain = domain.lower().lstrip("www.")
    paths = ["/contact", "/contact-us", "/about", "/team", "/staff", ""]
    api = "https://archive.org/wayback/available?url="
    out: list[dict] = []
    seen: set[str] = set()
    async with httpx.AsyncClient(
        timeout=20.0, follow_redirects=True, headers={"User-Agent": UA}
    ) as c:
        for p in paths[:max_pages]:
            target = f"{domain}{p}"
            try:
                r = await c.get(api + urllib.parse.quote(target))
                if r.status_code != 200:
                    continue
                data = r.json()
                snap = (data.get("archived_snapshots") or {}).get("closest") or {}
                snap_url = snap.get("url") or ""
                if not snap_url or not snap.get("available"):
                    continue
                rs = await c.get(snap_url)
                if rs.status_code != 200:
                    continue
                for e in _clean_emails(rs.text, domain):
                    if e in seen:
                        continue
                    seen.add(e)
                    out.append({
                        "email": e,
                        "source": "wayback",
                        "source_url": snap_url,
                        "confidence": 60,
                        "is_generic": e.split("@", 1)[0] in (
                            "info", "contact", "hello", "support", "office",
                            "admin", "inquiry", "billing",
                        ),
                    })
                if out:
                    break
            except Exception:
                continue
            await asyncio.sleep(0.3)
    return out


# ─── 2. Officer-targeted site-search ──────────────────────────────────
async def find_email_for_person_on_site(
    domain: str,
    first: str,
    last: str,
    max_pages: int = 4,
) -> list[dict]:
    """Bing for `site:<domain> "<First> <Last>"` then scrape emails."""
    if not domain or not (first or last):
        return []
    domain = domain.lower().lstrip("www.")
    person = f'"{first} {last}"' if first and last else f'"{first or last}"'
    q = f'site:{domain} {person}'
    bing = f"https://www.bing.com/search?q={urllib.parse.quote(q)}"

    async with httpx.AsyncClient(
        timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
    ) as c:
        try:
            r = await c.get(bing)
            if r.status_code != 200:
                return []
            html = r.text
        except Exception:
            return []
        urls = []
        for u in re.findall(r'<a[^>]+href="(https?://[^"\']+)"', html):
            host = u.split("/")[2].lower() if "://" in u else ""
            if domain not in host:
                continue
            if "bing.com" in host:
                continue
            urls.append(u)
            if len(urls) >= max_pages:
                break
        if not urls:
            return []

        out: list[dict] = []
        seen: set[str] = set()
        for u in urls:
            try:
                rp = await c.get(u)
                if rp.status_code != 200:
                    continue
                emails = _clean_emails(rp.text, domain)
                # Heuristic: prefer emails whose local-part contains
                # part of the person's first or last name.
                fl = (first or "").lower()
                ll = (last or "").lower()
                personal = [e for e in emails if (fl and fl in e.split("@", 1)[0])
                            or (ll and ll in e.split("@", 1)[0])]
                chosen = personal or emails
                for e in chosen:
                    if e in seen:
                        continue
                    seen.add(e)
                    is_personal = any((fl and fl in e), (ll and ll in e))
                    out.append({
                        "email": e,
                        "source": "person-site-search",
                        "source_url": u,
                        "confidence": 90 if is_personal else 70,
                        "is_generic": e.split("@", 1)[0] in (
                            "info", "contact", "hello", "support", "office",
                            "admin",
                        ),
                        "is_personal_match": bool(is_personal),
                    })
                if out:
                    break
            except Exception:
                continue
            await asyncio.sleep(0.3)
        return out


# ─── 3. Verified LinkedIn slug resolver ───────────────────────────────
async def resolve_linkedin_url(first: str, last: str, org: str) -> str:
    """Return the first linkedin.com/in/<slug> URL Bing finds, or ""."""
    if not (first and last):
        return ""
    q = f'"{first} {last}" "{org}" site:linkedin.com/in'
    url = f"https://www.bing.com/search?q={urllib.parse.quote(q)}"
    try:
        async with httpx.AsyncClient(
            timeout=15.0, follow_redirects=True, headers={"User-Agent": UA}
        ) as c:
            r = await c.get(url)
            if r.status_code != 200:
                return ""
            html = r.text
    except Exception:
        return ""
    m = re.search(r'href="(https?://[a-z]{0,3}\.?linkedin\.com/in/[^"\']+)"', html, re.I)
    if not m:
        return ""
    return _html.unescape(m.group(1))
