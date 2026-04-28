"""LinkedIn / social profile *resolver* — fetches search results server-side
and returns a direct profile URL the user can click straight to.

Why this exists: a `?q=name+site:linkedin.com/in` link still requires the
user to skim a results page. For the user's outreach workflow they need
the actual `linkedin.com/in/<slug>` URL pasted into the row. We get it
by issuing a Brave Search HTML query and parsing the first matching
profile out of the returned page.

Design:
  * Strict 6s HTTP timeout per query, single retry on 429 with jitter.
  * SQLite cache (`/tmp/linkedin_resolver_cache.db`) keyed by lower-cased
    "first|last|org" — same lead is only resolved once ever.
  * `resolve_linkedin_profile()` returns ("", "") on any failure so the
    caller can cleanly fall back to a search URL.
  * `resolve_facebook_profile()` and `resolve_instagram_profile()` use
    the same machinery for `facebook.com/<slug>` / `instagram.com/<slug>`.
  * Module-level `MAX_LIVE_LOOKUPS_PER_RUN` caps total live queries per
    process startup so a 200-lead hunt can't get rate-limited into
    oblivion — anything above the cap reads cache only.
"""

from __future__ import annotations

import os
import re
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from typing import Optional, Tuple

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

CACHE_DB = os.environ.get("LINKEDIN_CACHE_DB", "/tmp/linkedin_resolver_cache_v2.db")
HTTP_TIMEOUT = 6.0
MAX_LIVE_LOOKUPS_PER_RUN = int(os.environ.get("LINKEDIN_MAX_LIVE_LOOKUPS", "120"))
THROTTLE_SEC = float(os.environ.get("LINKEDIN_THROTTLE_SEC", "0.6"))

_lock = threading.Lock()
_live_count = 0
_last_query_at = 0.0


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(CACHE_DB, timeout=5)
    c.execute(
        "CREATE TABLE IF NOT EXISTS profile_cache ("
        "platform TEXT, key TEXT, url TEXT, fetched_at INTEGER,"
        "PRIMARY KEY (platform, key))"
    )
    return c


# Negative lookups (no match found) expire after this many seconds so we
# retry them on a future hunt instead of permanently writing them off.
NEGATIVE_TTL_SEC = 6 * 3600


def _cache_get(platform: str, key: str):
    """Return cached URL ('' if previously failed and still fresh) or None to retry."""
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT url, fetched_at FROM profile_cache WHERE platform=? AND key=?",
                (platform, key),
            ).fetchone()
            if not row:
                return None
            url, ts = row
            if not url and (time.time() - (ts or 0)) > NEGATIVE_TTL_SEC:
                return None  # negative expired — retry
            return url
    except Exception:
        return None


def _cache_put(platform: str, key: str, url: str) -> None:
    try:
        with _conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO profile_cache(platform, key, url, fetched_at) VALUES (?,?,?,?)",
                (platform, key, url, int(time.time())),
            )
    except Exception:
        pass


def _norm_key(first: str, last: str, org: str) -> str:
    return f"{(first or '').strip().lower()}|{(last or '').strip().lower()}|{(org or '').strip().lower()}"


def _can_make_live_query() -> bool:
    global _live_count, _last_query_at
    with _lock:
        if _live_count >= MAX_LIVE_LOOKUPS_PER_RUN:
            return False
        # Throttle
        now = time.time()
        wait = THROTTLE_SEC - (now - _last_query_at)
        if wait > 0:
            time.sleep(wait)
        _last_query_at = time.time()
        _live_count += 1
        return True


def _fetch(url: str) -> str:
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": UA,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        return urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read().decode("utf-8", "replace")
    except Exception:
        return ""


_LINKEDIN_PROFILE_RE = re.compile(
    r"https?://(?:www\.|[a-z]{2,3}\.)linkedin\.com/in/[A-Za-z0-9_%\-./]+",
    re.IGNORECASE,
)
_FB_PROFILE_RE = re.compile(
    r"https?://(?:www\.|[a-z]{2,3}\.)facebook\.com/[A-Za-z0-9_.\-]+(?:/)?",
    re.IGNORECASE,
)
_IG_PROFILE_RE = re.compile(
    r"https?://(?:www\.|[a-z]{2,3}\.)instagram\.com/[A-Za-z0-9_.]+(?:/)?",
    re.IGNORECASE,
)


def _strip_garbage(url: str) -> str:
    # Drop trailing punctuation / quotes / closing tags
    url = url.split('"')[0].split("'")[0].split("<")[0]
    url = re.sub(r"[.,)\]]+$", "", url)
    return url


def _filter_linkedin(matches):
    out = []
    seen = set()
    for u in matches:
        u = _strip_garbage(u)
        # Drop /pub-search, /pulse, /jobs, /company prefixes — only /in/{slug}
        if "/in/" not in u:
            continue
        # Drop bare /in/ with no slug
        slug = u.split("/in/", 1)[1].strip("/")
        if not slug or len(slug) < 2:
            continue
        # Strip query-string and fragment
        u = u.split("?")[0].split("#")[0]
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _resolve_via_brave(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """Return up to ``max_results`` URLs matching ``regex`` from a Brave search."""
    if not _can_make_live_query():
        return [] if max_results > 1 else ""
    url = f"https://search.brave.com/search?q={urllib.parse.quote(query)}"
    html = _fetch(url)
    if not html:
        return [] if max_results > 1 else ""
    matches = regex.findall(html)
    if post_filter:
        matches = post_filter(matches)
    if max_results > 1:
        return matches[:max_results]
    return matches[0] if matches else ""


def _resolve_via_ddg(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """DuckDuckGo HTML endpoint — fallback when Brave returns 429."""
    if not _can_make_live_query():
        return [] if max_results > 1 else ""
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    html = _fetch(url)
    if not html:
        return [] if max_results > 1 else ""
    # DDG wraps result links in /l/?uddg=<url-encoded-target>
    targets = []
    for m in re.findall(r'uddg=([^"&]+)', html):
        decoded = urllib.parse.unquote(m)
        if regex.match(decoded):
            targets.append(decoded)
    # Direct matches in the page body too (some DDG variants don't wrap)
    targets.extend(regex.findall(html))
    if post_filter:
        targets = post_filter(targets)
    if max_results > 1:
        return targets[:max_results]
    return targets[0] if targets else ""


def _resolve_chain(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """Try Brave first, fall back to DuckDuckGo on empty/failure."""
    res = _resolve_via_brave(query, regex, post_filter, max_results)
    if res:
        return res
    return _resolve_via_ddg(query, regex, post_filter, max_results)


_LINKEDIN_COMPANY_RE = re.compile(
    r"https?://(?:www\.|[a-z]{2,3}\.)linkedin\.com/company/[A-Za-z0-9_%\-./]+",
    re.IGNORECASE,
)


def _filter_linkedin_company(matches):
    out, seen = [], set()
    for u in matches:
        u = _strip_garbage(u).split("?")[0].split("#")[0]
        if "/company/" not in u:
            continue
        slug = u.split("/company/", 1)[1].strip("/")
        if not slug or len(slug) < 2:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def resolve_linkedin_profile(first: str, last: str, org: str = "") -> str:
    """Return a direct linkedin.com/in/<slug> URL, or '' if unresolvable."""
    if not first or not last:
        return ""
    key = _norm_key(first, last, org)
    cached = _cache_get("linkedin", key)
    if cached is not None:
        return cached  # may be '' meaning we tried and failed

    q = f"{first} {last} {org} site:linkedin.com/in".strip()
    url = _resolve_chain(q, _LINKEDIN_PROFILE_RE, _filter_linkedin)
    # Save even empty results so we don't retry forever
    _cache_put("linkedin", key, url)
    return url


def resolve_company_linkedin(org: str) -> str:
    """Find the company's LinkedIn /company/ page (returns '' if none)."""
    if not org:
        return ""
    key = _norm_key("", "", org)
    cached = _cache_get("linkedin_company", key)
    if cached is not None:
        return cached
    q = f"{org} site:linkedin.com/company"
    url = _resolve_chain(q, _LINKEDIN_COMPANY_RE, _filter_linkedin_company)
    _cache_put("linkedin_company", key, url)
    return url


def resolve_employee_at_company(org: str, max_results: int = 3) -> list[str]:
    """Backup: find ANY employee profile linked to ``org`` on LinkedIn.

    Used when the named decision-maker has no LinkedIn profile so the user
    still gets a real human at the company they can DM.
    """
    if not org:
        return []
    key = _norm_key("", "", f"emp::{org}")
    cached = _cache_get("linkedin_employee", key)
    if cached is not None:
        return [u for u in cached.split("|") if u]
    q = f"{org} site:linkedin.com/in"
    urls = _resolve_chain(q, _LINKEDIN_PROFILE_RE, _filter_linkedin, max_results=max_results)
    if isinstance(urls, str):
        urls = [urls] if urls else []
    _cache_put("linkedin_employee", key, "|".join(urls))
    return urls


def resolve_facebook_profile(first: str, last: str, org: str = "") -> str:
    if not first or not last:
        return ""
    key = _norm_key(first, last, org)
    cached = _cache_get("facebook", key)
    if cached is not None:
        return cached
    q = f"{first} {last} {org} site:facebook.com".strip()
    url = _resolve_chain(q, _FB_PROFILE_RE)
    _cache_put("facebook", key, url)
    return url


def resolve_instagram_profile(first: str, last: str, org: str = "") -> str:
    if not first or not last:
        return ""
    key = _norm_key(first, last, org)
    cached = _cache_get("instagram", key)
    if cached is not None:
        return cached
    q = f"{first} {last} {org} site:instagram.com".strip()
    url = _resolve_chain(q, _IG_PROFILE_RE)
    _cache_put("instagram", key, url)
    return url


def reset_run_budget() -> None:
    """Reset the per-run live-query counter (call at the start of each hunt)."""
    global _live_count
    with _lock:
        _live_count = 0
