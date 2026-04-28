"""LinkedIn outreach helpers — pre-filled search URLs + DM templates.

Approach: instead of scraping LinkedIn (ToS violation, ban risk) or
search engines (CAPTCHA-blocked from cloud IPs), we build *clickable
search URLs* that open in the user's already-logged-in LinkedIn
session. The user clicks one link, sees real verified profiles in
their own LinkedIn UI, picks the right one, and pastes our
pre-written DM.

Zero scraping. Zero API costs. Zero risk to user's LinkedIn account.
"""

from __future__ import annotations

from urllib.parse import quote, urlencode
from typing import Optional


def linkedin_people_search_url(first: str, last: str, org: str = "") -> str:
    """Profile lookup via Bing site-search — no consent wall, no login."""
    parts = [first, last]
    if org:
        parts.append(org)
    parts.append("site:linkedin.com/in")
    q = " ".join(p for p in parts if p)
    return f"https://www.bing.com/search?q={quote(q)}"


def linkedin_native_search_url(first: str, last: str, org: str = "") -> str:
    """Native LinkedIn URL — only useful if user has Premium / Sales Nav."""
    keywords = f"{first} {last}".strip()
    if org:
        keywords = f"{keywords} {org}".strip()
    return (
        "https://www.linkedin.com/search/results/people/?"
        + urlencode({"keywords": keywords, "origin": "GLOBAL_SEARCH_HEADER"})
    )


def linkedin_sales_nav_url(first: str, last: str, org: str = "") -> str:
    """Sales Navigator URL — for users with Sales Nav subscription."""
    keywords = f"{first} {last}".strip()
    if org:
        keywords = f"{keywords} {org}".strip()
    return "https://www.linkedin.com/sales/search/people?" + urlencode({"keywords": keywords})


def google_linkedin_url(first: str, last: str, org: str = "") -> str:
    """Google search constrained to linkedin.com/in/ — opens in browser."""
    q = f'"{first} {last}"'
    if org:
        q += f' "{org}"'
    q += " site:linkedin.com/in"
    return f"https://www.google.com/search?q={quote(q)}"


def linkedin_outreach_template(
    first: str,
    org: str,
    sender_name: str = "Eric",
    sender_company: str = "MedPharma SC",
    service_pitch: str = "billing & credentialing for diagnostic labs",
) -> dict:
    """Personalized, ready-to-send LinkedIn DM + connection note.

    Returns {connection_note (≤300 chars), first_message, follow_up}.
    """
    first = (first or "").split()[0].strip().title() or "there"
    org = (org or "").strip() or "your organization"
    sender = sender_name.strip() or "Eric"

    note = (
        f"Hi {first} — {sender} here. I work with diagnostic labs on "
        f"{service_pitch} and came across {org}. Would love to connect."
    )
    if len(note) > 295:
        note = note[:292] + "..."

    msg = (
        f"Hi {first},\n\n"
        f"Quick context: I run {sender_company} — we handle {service_pitch} "
        f"for independent labs. Most clients come to us when they're losing "
        f"10–18% of net revenue to denials, slow A/R, or credentialing gaps.\n\n"
        f"Saw you're at {org}. Worth a 15-minute call to see if there's a fit? "
        f"No pitch — I'll just walk through what we'd look at first.\n\n"
        f"— {sender}"
    )

    follow = (
        f"Hi {first} — circling back. If RCM isn't a priority right now, "
        f"totally understand. If it is, I can send a one-page snapshot of "
        f"what we typically find on a 15-minute review. Just say the word."
    )
    return {
        "connection_note": note,
        "first_message": msg,
        "follow_up": follow,
    }


async def find_linkedin_profile(
    first: str,
    last: str,
    org: str = "",
    title: str = "",
    client=None,
) -> Optional[dict]:
    """Returns a set of clickable LinkedIn/Google search URLs.

    No scraping — these always work. The user clicks the link in their
    browser (already logged into LinkedIn) and sees real profiles.
    """
    if not first or not last:
        return None
    return {
        "url": linkedin_people_search_url(first, last, org),
        "sales_nav_url": linkedin_sales_nav_url(first, last, org),
        "google_url": google_linkedin_url(first, last, org),
        "source": "search_url",
    }

