"""Social outreach helpers — multi-platform DM URLs + message templates.

Strategy: spam filters destroy cold email. Direct messages land.
We generate clickable search/DM URLs for every major platform — the
user clicks one, opens the platform in their already-logged-in
browser session, finds the right account, and pastes our pre-written
message. Zero scraping, zero API costs, zero account-ban risk.

Platforms covered:
  • LinkedIn (person + company)
  • Facebook (page search — most healthcare orgs have FB pages)
  • Instagram (search by name/handle)
  • X / Twitter (search)
  • Google site-search across all platforms (universal fallback)
  • SMS template (we already have direct phone lines from NPI)
"""

from __future__ import annotations

from urllib.parse import quote, urlencode
from typing import Optional


# ─── Person-level search URLs ──────────────────────────────────────────

def linkedin_people_search_url(first: str, last: str, org: str = "") -> str:
    """LinkedIn profile lookup via Bing site-search.

    Why Bing instead of Google or LinkedIn's own search:
      * LinkedIn's `/search/results/people/` requires login (Premium gated
        for full results) and shows nothing for anonymous visitors.
      * Google often serves a cookie-consent interstitial that blocks the
        result list until the user clicks through, and is more aggressive
        about CAPTCHA from cloud regions.
      * Bing returns site-restricted results immediately, no consent wall.

    The query intentionally drops the strict quoting on the full name so
    Bing surfaces partial matches — important because small lab owners
    often appear on LinkedIn under a slightly different first-name spelling
    (Bob/Robert, Mike/Michael, etc.).
    """
    parts = [first, last]
    if org:
        parts.append(org)
    parts.append("site:linkedin.com/in")
    q = " ".join(p for p in parts if p)
    return f"https://www.bing.com/search?q={quote(q)}"


def linkedin_native_search_url(first: str, last: str, org: str = "") -> str:
    """Native LinkedIn people-search URL (works for Premium / Sales Nav users)."""
    keywords = f"{first} {last}".strip()
    if org:
        keywords = f"{keywords} {org}".strip()
    return (
        "https://www.linkedin.com/search/results/people/?"
        + urlencode({"keywords": keywords, "origin": "GLOBAL_SEARCH_HEADER"})
    )


def linkedin_sales_nav_url(first: str, last: str, org: str = "") -> str:
    """Sales Navigator URL — for users with Sales Nav."""
    keywords = f"{first} {last}".strip()
    if org:
        keywords = f"{keywords} {org}".strip()
    return "https://www.linkedin.com/sales/search/people?" + urlencode({"keywords": keywords})


def facebook_people_search_url(first: str, last: str, org: str = "") -> str:
    """Facebook profile lookup via Bing site-search.

    FB's `/public/{Name}` page works but only shows ~5 results and skips
    profiles whose privacy excludes them from public listing. A Bing
    `site:facebook.com` query surfaces business pages, public posts, and
    profile fragments that FB's own slug page hides.
    """
    parts = [first, last]
    if org:
        parts.append(org)
    parts.append("site:facebook.com")
    q = " ".join(p for p in parts if p)
    return f"https://www.bing.com/search?q={quote(q)}"


def facebook_google_search_url(first: str, last: str, org: str = "") -> str:
    """Bing fallback when FB's own search is rate-limited."""
    parts = [first, last]
    if org:
        parts.append(org)
    parts.append("site:facebook.com")
    q = " ".join(p for p in parts if p)
    return f"https://www.bing.com/search?q={quote(q)}"


def instagram_search_url(first: str, last: str, org: str = "") -> str:
    """Instagram profile lookup via Bing site-search (no consent wall)."""
    parts = [first, last]
    if org:
        parts.append(org)
    parts.append("site:instagram.com")
    q = " ".join(p for p in parts if p)
    return f"https://www.bing.com/search?q={quote(q)}"


def x_twitter_search_url(first: str, last: str, org: str = "") -> str:
    """X (Twitter) people search."""
    q = f'"{first} {last}"'
    if org:
        q += f" {org}"
    return f"https://x.com/search?{urlencode({'q': q, 'f': 'user'})}"


def google_linkedin_url(first: str, last: str, org: str = "") -> str:
    """Bing constrained to linkedin.com/in/."""
    parts = [first, last]
    if org:
        parts.append(org)
    parts.append("site:linkedin.com/in")
    q = " ".join(p for p in parts if p)
    return f"https://www.bing.com/search?q={quote(q)}"


def google_social_url(first: str, last: str, org: str = "") -> str:
    """Bing search across LinkedIn + Facebook + Instagram + X for a person."""
    parts = [first, last]
    if org:
        parts.append(org)
    q = " ".join(p for p in parts if p)
    q += " (site:linkedin.com OR site:facebook.com OR site:instagram.com OR site:x.com OR site:twitter.com)"
    return f"https://www.bing.com/search?q={quote(q)}"


# ─── Company-level URLs (when person can't be found) ──────────────────

def linkedin_company_search_url(org: str) -> str:
    """Find the company's LinkedIn page via Bing site-search."""
    if not org:
        return ""
    q = f"{org} site:linkedin.com/company"
    return f"https://www.bing.com/search?q={quote(q)}"


def facebook_page_search_url(org: str) -> str:
    """Find the company's Facebook page via Bing site-search."""
    if not org:
        return ""
    return f"https://www.bing.com/search?q={quote(f'{org} site:facebook.com')}"


def instagram_company_search_url(org: str) -> str:
    """Find the company's IG handle via Bing site-search."""
    if not org:
        return ""
    return f"https://www.bing.com/search?q={quote(f'{org} site:instagram.com')}"


def google_company_social_url(org: str) -> str:
    """Find the company's social presence across platforms (via Bing)."""
    if not org:
        return ""
    q = f"{org} (site:linkedin.com/company OR site:facebook.com OR site:instagram.com OR site:x.com OR site:twitter.com)"
    return f"https://www.bing.com/search?q={quote(q)}"


# ─── Message templates ─────────────────────────────────────────────────

def social_outreach_templates(
    first: str,
    org: str,
    sender_name: str = "Eric",
    sender_company: str = "MedPharma SC",
    service_pitch: str = "billing & credentialing for diagnostic labs",
) -> dict:
    """Generate platform-tuned messages.

    Returns:
      linkedin_connection_note (≤300 chars)
      linkedin_first_message
      linkedin_follow_up
      facebook_dm        (shorter, more casual — FB users skim)
      instagram_dm       (very short — IG DMs are mobile-first)
      x_dm               (≤280 chars to fit X's tone)
      sms                (≤160 chars, one-shot)
    """
    first = (first or "").split()[0].strip().title() or "there"
    org = (org or "").strip() or "your organization"
    sender = sender_name.strip() or "Eric"

    # LinkedIn — professional, longer
    li_note = (
        f"Hi {first} — {sender} here. I work with diagnostic labs on "
        f"{service_pitch} and came across {org}. Would love to connect."
    )
    if len(li_note) > 295:
        li_note = li_note[:292] + "..."

    li_msg = (
        f"Hi {first},\n\n"
        f"Quick context: I run {sender_company} — we handle {service_pitch} "
        f"for independent labs. Most clients come to us when they're losing "
        f"10–18% of net revenue to denials, slow A/R, or credentialing gaps.\n\n"
        f"Saw you're at {org}. Worth a 15-minute call to see if there's a fit? "
        f"No pitch — I'll just walk through what we'd look at first.\n\n"
        f"— {sender}"
    )

    li_follow = (
        f"Hi {first} — circling back. If RCM isn't a priority right now, "
        f"totally understand. If it is, I can send a one-page snapshot of "
        f"what we typically find on a 15-min review. Just say the word."
    )

    # Facebook — friendly, page DM tone
    fb = (
        f"Hi {first}, I work with diagnostic labs on RCM (billing & "
        f"credentialing) and came across {org}. Quick question — do you "
        f"handle the revenue cycle in-house or outsource it? Happy to share "
        f"a free 15-min review of where labs typically leak revenue. "
        f"— {sender}, {sender_company}"
    )

    # Instagram — mobile, brief
    ig = (
        f"Hey {first} 👋 I run {sender_company} — we help labs like {org} "
        f"recover lost revenue from denials & slow A/R. Open to a quick "
        f"15-min chat? — {sender}"
    )

    # X / Twitter — punchy
    x_msg = (
        f"Hey {first} — quick one. We help diagnostic labs recover 10–18% "
        f"of net revenue lost to denials & A/R. Saw {org}. 15-min call "
        f"worth it? — {sender}, {sender_company}"
    )
    if len(x_msg) > 275:
        x_msg = x_msg[:272] + "..."

    # SMS — single-message length, no fluff
    sms = (
        f"Hi {first}, {sender} w/ {sender_company}. We handle billing "
        f"& credentialing for labs — typically recover 10-18% of lost "
        f"revenue. 15-min call to see fit? Reply STOP to opt out."
    )
    if len(sms) > 160:
        sms = sms[:157] + "..."

    return {
        "linkedin_connection_note": li_note,
        "linkedin_first_message":   li_msg,
        "linkedin_follow_up":       li_follow,
        "facebook_dm": fb,
        "instagram_dm": ig,
        "x_dm": x_msg,
        "sms": sms,
    }


# ─── Unified API used by scrubber ──────────────────────────────────────

async def find_social_profiles(
    first: str,
    last: str,
    org: str = "",
    title: str = "",
    client=None,
) -> Optional[dict]:
    """Build clickable search URLs for every major social platform.

    No scraping. The user clicks the URL in their browser and lands on
    the platform's own search page (already logged in).
    """
    if not first or not last:
        # Person unknown — return company-only URLs
        if not org:
            return None
        return {
            "linkedin_company_url":  linkedin_company_search_url(org),
            "facebook_page_url":     facebook_page_search_url(org),
            "instagram_company_url": instagram_company_search_url(org),
            "google_company_social": google_company_social_url(org),
        }

    return {
        # Person-level
        "linkedin_url":          linkedin_people_search_url(first, last, org),
        "linkedin_sales_nav":    linkedin_sales_nav_url(first, last, org),
        "facebook_url":          facebook_people_search_url(first, last, org),
        "instagram_url":         instagram_search_url(first, last, org),
        "x_url":                 x_twitter_search_url(first, last, org),
        "google_linkedin":       google_linkedin_url(first, last, org),
        "google_social":         google_social_url(first, last, org),
        # Company-level fallbacks
        "linkedin_company_url":  linkedin_company_search_url(org) if org else "",
        "facebook_page_url":     facebook_page_search_url(org) if org else "",
        "instagram_company_url": instagram_company_search_url(org) if org else "",
    }


# ─── Backwards-compat shims for existing scrubber imports ─────────────

async def find_linkedin_profile(first, last, org="", title="", client=None):
    """Legacy alias — returns LinkedIn-shaped dict."""
    if not first or not last:
        return None
    return {
        "url":           linkedin_people_search_url(first, last, org),
        "sales_nav_url": linkedin_sales_nav_url(first, last, org),
        "google_url":    google_linkedin_url(first, last, org),
        "source":        "search_url",
    }


def linkedin_outreach_template(first, org, sender_name="Eric",
                                sender_company="MedPharma SC",
                                service_pitch="billing & credentialing for diagnostic labs"):
    """Legacy alias — returns LinkedIn-only fields."""
    t = social_outreach_templates(first, org, sender_name, sender_company, service_pitch)
    return {
        "connection_note": t["linkedin_connection_note"],
        "first_message":   t["linkedin_first_message"],
        "follow_up":       t["linkedin_follow_up"],
    }
