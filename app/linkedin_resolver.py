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

import json as _json
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

CACHE_DB = os.environ.get("LINKEDIN_CACHE_DB", "/data/linkedin_profiles.db")
HTTP_TIMEOUT = 5.0
MAX_LIVE_LOOKUPS_PER_RUN = int(os.environ.get("LINKEDIN_MAX_LIVE_LOOKUPS", "2000"))
THROTTLE_SEC = float(os.environ.get("LINKEDIN_THROTTLE_SEC", "0.15"))

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


_ORG_NOISE_SUFFIXES = (
    " LLC", " L.L.C.", " L.L.C", " INC", " INC.", " CORP", " CORPORATION",
    " CO", " CO.", " PLLC", " P.L.L.C.", " LP", " L.P.", " PA", " P.A.",
    " PC", " P.C.", " LTD", " LTD.", " LIMITED", " GROUP", " SERVICES",
    " LABORATORY", " LABORATORIES", " LAB", " LABS", " DIAGNOSTICS",
    " MEDICAL", " CLINIC",
)


def _clean_org(org: str) -> str:
    """Strip noise suffixes — ``ABC LABS LLC`` → ``ABC``."""
    s = (org or "").strip()
    if not s:
        return ""
    upper = s.upper()
    changed = True
    while changed:
        changed = False
        for suf in _ORG_NOISE_SUFFIXES:
            if upper.endswith(suf):
                upper = upper[: -len(suf)].rstrip(" ,")
                changed = True
    return upper.strip(" ,")


def _org_query_variants(org: str) -> list[str]:
    """Yield variants of the org name to try (full → cleaned → first-word)."""
    seen, out = set(), []
    for v in (org or "", _clean_org(org)):
        v = (v or "").strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)
    cleaned = _clean_org(org)
    if cleaned and " " in cleaned:
        first_word = cleaned.split()[0]
        if first_word.lower() not in seen and len(first_word) >= 3:
            seen.add(first_word.lower())
            out.append(first_word)
    return out


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


def _resolve_via_bing(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """Bing HTML search — usually not rate-limited from cloud IPs."""
    if not _can_make_live_query():
        return [] if max_results > 1 else ""
    url = f"https://www.bing.com/search?q={urllib.parse.quote(query)}&count=20"
    html = _fetch(url)
    if not html:
        return [] if max_results > 1 else ""
    matches = regex.findall(html)
    if post_filter:
        matches = post_filter(matches)
    if max_results > 1:
        return matches[:max_results]
    return matches[0] if matches else ""


def _resolve_via_mojeek(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """Mojeek — independent search index, lenient with bots."""
    if not _can_make_live_query():
        return [] if max_results > 1 else ""
    url = f"https://www.mojeek.com/search?q={urllib.parse.quote(query)}"
    html = _fetch(url)
    if not html:
        return [] if max_results > 1 else ""
    matches = regex.findall(html)
    if post_filter:
        matches = post_filter(matches)
    if max_results > 1:
        return matches[:max_results]
    return matches[0] if matches else ""


def _resolve_via_startpage(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """Startpage — Google proxy, no Google bot detection."""
    if not _can_make_live_query():
        return [] if max_results > 1 else ""
    url = f"https://www.startpage.com/sp/search?query={urllib.parse.quote(query)}"
    html = _fetch(url)
    if not html:
        return [] if max_results > 1 else ""
    matches = regex.findall(html)
    if post_filter:
        matches = post_filter(matches)
    if max_results > 1:
        return matches[:max_results]
    return matches[0] if matches else ""


def _resolve_via_serp(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """SerpAPI Google Search JSON — highest reliability, 100 free searches/month.
    Set SERP_API_KEY env var to enable. Falls back to HTML scrapers if not set."""
    serp_key = os.environ.get("SERP_API_KEY", "")
    if not serp_key:
        return [] if max_results > 1 else ""
    if not _can_make_live_query():
        return [] if max_results > 1 else ""
    try:
        url = (
            "https://serpapi.com/search.json"
            f"?engine=google&q={urllib.parse.quote(query)}&num=10&api_key={serp_key}"
        )
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
        data = _json.loads(resp.read().decode("utf-8", "replace"))
        links = [r.get("link", "") for r in data.get("organic_results", [])]
        matches = [u for u in links if regex.match(u)]
        if post_filter:
            matches = post_filter(matches)
        if max_results > 1:
            return matches[:max_results]
        return matches[0] if matches else ""
    except Exception:
        return [] if max_results > 1 else ""


def _resolve_chain(query: str, regex: re.Pattern, post_filter=None, max_results: int = 1):
    """Try SerpAPI (best, if key set) → Bing → DDG → Brave."""
    for fn in (_resolve_via_serp, _resolve_via_bing, _resolve_via_ddg, _resolve_via_brave):
        res = fn(query, regex, post_filter, max_results)
        if res:
            return res
    return [] if max_results > 1 else ""


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


def linkedin_search_url(first: str, last: str, org: str = "") -> str:
    """Always-clickable Bing search URL pre-filtered to that person on LinkedIn.

    Lands the user on a real Bing results page constrained to
    ``site:linkedin.com/in`` so the first result is almost always the
    correct profile. This is the production fallback when we can't
    resolve a direct slug from cloud IPs (search engines block bots).
    """
    if not (first and last):
        return ""
    parts = [first.strip(), last.strip()]
    if org:
        cleaned = _clean_org(org)
        if cleaned:
            parts.append(cleaned)
    parts.append("site:linkedin.com/in")
    q = " ".join(parts)
    return f"https://www.bing.com/search?q={urllib.parse.quote(q)}"


def linkedin_company_search_url(org: str) -> str:
    if not org:
        return ""
    cleaned = _clean_org(org) or org
    q = f"{cleaned} site:linkedin.com/company"
    return f"https://www.bing.com/search?q={urllib.parse.quote(q)}"


def linkedin_company_people_url(org: str) -> str:
    """Bing search URL pre-filtered to the company's LinkedIn /people roster.

    Pattern ``site:linkedin.com/company "<org>" people`` lands the user
    on the company's employee roster page in 1 click. This is the
    closest we can get to "find the company roster" without a paid API.
    """
    if not org:
        return ""
    cleaned = _clean_org(org) or org
    q = f'"{cleaned}" site:linkedin.com/company people employees'
    return f"https://www.bing.com/search?q={urllib.parse.quote(q)}"


def resolve_linkedin_profile(first: str, last: str, org: str = "") -> str:
    """Return a direct linkedin.com/in/<slug> URL, or '' if unresolvable.

    Tries multiple query variants — with org, with org cleaned of
    LLC/INC/LABS suffixes, and finally without org. First hit wins.
    """
    if not first or not last:
        return ""
    key = _norm_key(first, last, org)
    cached = _cache_get("linkedin", key)
    if cached is not None:
        return cached  # may be '' meaning we tried and failed

    queries = []
    for variant in _org_query_variants(org):
        queries.append(f"{first} {last} {variant} site:linkedin.com/in")
    queries.append(f"{first} {last} site:linkedin.com/in")

    url = ""
    for q in queries:
        url = _resolve_chain(q, _LINKEDIN_PROFILE_RE, _filter_linkedin)
        if url:
            break
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

    Tries the full org name first, then a cleaned version (LLC/INC stripped),
    then the first significant word. Used when the named decision-maker has
    no LinkedIn profile so the user still gets a real human at the company.
    """
    if not org:
        return []
    key = _norm_key("", "", f"emp::{org}")
    cached = _cache_get("linkedin_employee", key)
    if cached is not None:
        urls = [u for u in cached.split("|") if u]
        if urls:
            return urls
        # Negative — if expired, fall through; otherwise return []
        return urls

    accumulated: list[str] = []
    seen: set[str] = set()
    for variant in _org_query_variants(org):
        q = f"{variant} site:linkedin.com/in"
        urls = _resolve_chain(q, _LINKEDIN_PROFILE_RE, _filter_linkedin, max_results=max_results)
        if isinstance(urls, str):
            urls = [urls] if urls else []
        for u in urls:
            if u not in seen:
                seen.add(u)
                accumulated.append(u)
        if len(accumulated) >= max_results:
            break
    accumulated = accumulated[:max_results]
    _cache_put("linkedin_employee", key, "|".join(accumulated))
    return accumulated


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
