"""Rule-based outreach email + LinkedIn DM copy generator.

Zero third-party dependencies. Generates 3 email sequences + LinkedIn DM
from lead enrichment data already in the outreach queue.
"""
from __future__ import annotations

import random

_SENDER_NAME = "Eric"
_SENDER_TITLE = "MedPharma SC"
_SENDER_PHONE = "(803) 626-3500"

# Pain signals mapped from tier + heat to concise copy hooks
_HOOKS_BY_TIER: dict[str, list[str]] = {
    "A": [
        "high-volume labs like yours often leave 15–20% of reimbursements on the table due to credentialing gaps",
        "independent labs with 50+ monthly claims typically see 12–18% denial rates that go unworked",
        "labs in your category are among the most denied by commercial payors for avoidable reasons",
    ],
    "B": [
        "growing labs often hit a credentialing bottleneck that slows payor enrollment by 60–90 days",
        "mid-size independent labs frequently struggle with ERA/EFT setup that delays payments unnecessarily",
        "labs expanding into new specialties often face enrollment delays that stall new revenue streams",
    ],
    "C": [
        "smaller independent labs often lack the bandwidth to stay ahead of payor credentialing renewals",
        "new labs frequently underestimate how long payor enrollment takes — we cut that timeline in half",
        "boutique labs benefit most from outsourced credentialing to avoid costly in-network delays",
    ],
    "": [
        "independent labs commonly lose revenue to avoidable credentialing and billing gaps",
        "payor enrollment delays are one of the top reasons labs lose reimbursements in the first 90 days",
    ],
}

_VALUE_PROPS = [
    "We handle full-cycle credentialing, payor enrollment, and billing for independent labs — so your team can focus on the science.",
    "MedPharma SC specializes in revenue cycle management for independent and specialty labs — credentialing to collections.",
    "We've helped labs like yours cut denial rates and speed up payor payments without adding headcount.",
]

_CTA_OPTIONS = [
    "Worth a 15-minute call this week?",
    "Open to a quick 10-minute chat to see if there's a fit?",
    "Could we find 15 minutes this week to compare notes?",
    "Would a short call make sense — no pitch, just a quick exchange?",
]

_SUBJECT_TEMPLATES = {
    1: [
        "Quick question for {first} at {org}",
        "{first} — lab credentialing question",
        "Revenue cycle gap I spotted for {org}",
        "{org} — billing question",
    ],
    2: [
        "Re: Quick question for {first}",
        "Following up — {org}",
        "{first}, still worth connecting?",
    ],
    3: [
        "Last note — {org}",
        "Closing the loop, {first}",
        "Final follow-up — {org}",
    ],
}


def _pick(items: list, seed: str = "") -> str:
    if not items:
        return ""
    idx = sum(ord(c) for c in seed) % len(items)
    return items[idx]


_HONORIFICS = {"dr.", "dr", "mr.", "mr", "mrs.", "mrs", "ms.", "ms", "prof.", "prof"}


def _first_name(decision_maker: str) -> str:
    parts = [p for p in str(decision_maker or "").split() if p]
    # Skip honorifics to get given name
    for part in parts:
        if part.lower().rstrip(".") not in _HONORIFICS and not part.endswith("."):
            return part
    return parts[0] if parts else "there"


def _hook(tier: str, org: str) -> str:
    hooks = _HOOKS_BY_TIER.get(str(tier).upper(), _HOOKS_BY_TIER[""])
    return _pick(hooks, org)


def generate_sequence(row: dict) -> dict[str, dict[str, str]]:
    """Generate a 3-step email sequence + LinkedIn DM for a single outreach queue row.

    Returns a dict with keys "email_1", "email_2", "email_3", "linkedin_dm".
    Each value is {"subject": ..., "body": ...}.
    """
    org = str(row.get("Org Name") or row.get("org_name") or "your lab").strip()
    dm = str(row.get("Decision Maker") or row.get("decision_maker") or "").strip()
    title = str(row.get("Title") or row.get("title") or "").strip()
    tier = str(row.get("Tier") or row.get("tier") or "").strip().upper()
    city = str(row.get("City") or row.get("city") or "").strip()
    state = str(row.get("State") or row.get("state") or "").strip()
    channel = str(row.get("Outreach Channel") or row.get("outreach_channel") or "email").strip().lower()

    first = _first_name(dm)
    hook = _hook(tier, org)
    value_prop = _pick(_VALUE_PROPS, org)
    cta = _pick(_CTA_OPTIONS, dm)
    loc = f"{city}, {state}" if city and state else state or city or ""
    org_with_loc = f"{org} ({loc})" if loc else org

    # Email 1 — Cold intro
    subj1 = _pick(_SUBJECT_TEMPLATES[1], org)
    subj1 = subj1.replace("{first}", first).replace("{org}", org)
    body1 = (
        f"Hi {first},\n\n"
        f"I came across {org_with_loc} and wanted to reach out — {hook}.\n\n"
        f"{value_prop}\n\n"
        f"{cta}\n\n"
        f"— {_SENDER_NAME}\n"
        f"{_SENDER_TITLE} | {_SENDER_PHONE}"
    )

    # Email 2 — Day 3 follow-up (different angle)
    subj2 = _pick(_SUBJECT_TEMPLATES[2], org)
    subj2 = subj2.replace("{first}", first).replace("{org}", org)
    body2 = (
        f"Hi {first},\n\n"
        f"Following up on my note from a couple days ago about {org}.\n\n"
        f"To give you a concrete idea: labs we work with typically see a 15–25% reduction in claim denials "
        f"within the first 90 days and faster payor enrollment by 4–6 weeks on average.\n\n"
        f"I know your time is limited — even a 10-minute call would be worth it. "
        f"Happy to work around your schedule.\n\n"
        f"— {_SENDER_NAME}\n"
        f"{_SENDER_TITLE} | {_SENDER_PHONE}"
    )

    # Email 3 — Day 7 breakup
    subj3 = _pick(_SUBJECT_TEMPLATES[3], org)
    subj3 = subj3.replace("{first}", first).replace("{org}", org)
    body3 = (
        f"Hi {first},\n\n"
        f"Last note, I promise.\n\n"
        f"If now isn't the right time for {org}, no worries at all — I completely understand. "
        f"I'll leave the door open if you ever want to revisit credentialing or revenue cycle support.\n\n"
        f"Wishing you and the team the best.\n\n"
        f"— {_SENDER_NAME}\n"
        f"{_SENDER_TITLE} | {_SENDER_PHONE}"
    )

    # LinkedIn DM (shorter, more direct)
    dm_name_label = f"{first}" if first != "there" else "Hi"
    dm_intro = f"{dm_name_label}, I noticed {org}" if first != "there" else f"I noticed {org}"
    linkedin_body = (
        f"{dm_intro} and wanted to connect — we work with independent labs on credentialing and revenue cycle. "
        f"Would love to share one idea that's helped similar labs reduce denials. Open to connecting?"
    )

    return {
        "email_1": {"subject": subj1, "body": body1},
        "email_2": {"subject": subj2, "body": body2},
        "email_3": {"subject": subj3, "body": body3},
        "linkedin_dm": {"subject": "", "body": linkedin_body},
    }


def generate_sequence_for_queue_id(queue_id: int) -> dict | None:
    """Load a queue row from DB and generate its outreach sequence."""
    from app.database import get_db
    import json as _json
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT payload_json FROM outreach_queue WHERE id = ?",
            (queue_id,),
        ).fetchone()
        if not row:
            return None
        payload = _json.loads(row[0] or "{}")
        return generate_sequence(payload)
    finally:
        conn.close()
