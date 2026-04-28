"""SMTP / MX deliverability check — pre-filter emails to cut bounce rate.

For each candidate email:
  1. Resolve MX records for the domain
  2. Open SMTP to the lowest-priority MX
  3. Issue HELO + MAIL FROM + RCPT TO
  4. Read response code:
        2xx        → DELIVERABLE
        5xx        → BOUNCE (drop the email)
        4xx        → UNKNOWN (keep, but mark)
        timeout    → UNKNOWN (keep, but mark)

Many providers (Gmail, Outlook, GSuite) accept all RCPT TOs and bounce
later, so we treat them as UNKNOWN. We only definitively bounce 5xx.

This is a synchronous network operation — runs in a thread to avoid
blocking the event loop.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import smtplib
import socket
from typing import Literal, Optional

log = logging.getLogger(__name__)

DeliverableStatus = Literal["deliverable", "bounce", "unknown", "skip"]


def _mx_records(domain: str) -> list[tuple[int, str]]:
    try:
        import dns.resolver  # type: ignore
    except ImportError:
        return []
    try:
        ans = dns.resolver.resolve(domain, "MX", lifetime=5.0)
        out = []
        for r in ans:
            try:
                out.append((int(r.preference), str(r.exchange).rstrip(".")))
            except Exception:
                continue
        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []


_PERMISSIVE_MAIL_PROVIDERS = (
    "google.com", "googlemail.com", "gsuite.com",
    "outlook.com", "hotmail.com", "live.com",
    "office365.com", "protection.outlook.com",
)


def _check_one(email: str, sender: str = "verify@medpharma-sc.com",
               timeout: float = 8.0) -> DeliverableStatus:
    if "@" not in email:
        return "skip"
    domain = email.split("@", 1)[1].strip().lower()
    if not domain or "." not in domain:
        return "skip"

    mxs = _mx_records(domain)
    if not mxs:
        # No MX → some domains accept on A record but most don't
        return "bounce"

    mx_host = mxs[0][1]
    # Permissive providers accept everything → unknown, not deliverable
    permissive = any(p in mx_host.lower() for p in _PERMISSIVE_MAIL_PROVIDERS)

    try:
        with contextlib.closing(smtplib.SMTP(timeout=timeout)) as srv:
            srv.connect(mx_host, 25)
            srv.helo("medpharma-sc.com")
            srv.mail(sender)
            code, _ = srv.rcpt(email)
            srv.quit() if False else None  # closing handles QUIT
    except (smtplib.SMTPException, socket.error, socket.timeout, OSError):
        return "unknown"
    except Exception:
        return "unknown"

    if 200 <= code < 300:
        return "unknown" if permissive else "deliverable"
    if 400 <= code < 500:
        return "unknown"
    if 500 <= code < 600:
        return "bounce"
    return "unknown"


async def check_deliverability(email: str) -> DeliverableStatus:
    if not email or "@" not in email:
        return "skip"
    return await asyncio.to_thread(_check_one, email)


async def filter_deliverable(emails: list[str]) -> list[tuple[str, DeliverableStatus]]:
    """Return (email, status) pairs. Drop status=='bounce'."""
    if not emails:
        return []
    tasks = [check_deliverability(e) for e in emails]
    statuses = await asyncio.gather(*tasks, return_exceptions=True)
    out: list[tuple[str, DeliverableStatus]] = []
    for e, s in zip(emails, statuses):
        if isinstance(s, Exception):
            out.append((e, "unknown"))
        else:
            out.append((e, s))
    return [(e, s) for e, s in out if s != "bounce"]
