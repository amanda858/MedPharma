import json
import os
import re
import socket
import time
from urllib.parse import urlparse
import httpx

PRIMARY_BASE = os.getenv("LEADS_BASE_URL", "https://medpharmahub.com").rstrip("/")
FALLBACK_BASE = "https://medpharma-hub.onrender.com"


def _host_resolves(url: str) -> bool:
    host = (urlparse(url).hostname or "").strip()
    if not host:
        return False
    try:
        socket.getaddrinfo(host, 443)
        return True
    except Exception:
        return False


BASE = PRIMARY_BASE if _host_resolves(PRIMARY_BASE) else FALLBACK_BASE
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
ROLE_PREFIXES = {
    "info", "contact", "support", "sales", "admin", "office", "billing", "help", "service", "team"
}


def _domain_resolves(domain: str) -> bool:
    domain = str(domain or "").strip().lower()
    if not domain:
        return False
    try:
        socket.getaddrinfo(domain, 80)
        return True
    except Exception:
        return False


def main():
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        login = client.post(
            f"{BASE}/hub/api/login",
            json={"username": "admin", "password": "admin123"},
        )
        broad = client.get(
            f"{BASE}/admin/leads/api/leads?quality_only=false&need_signal_only=false&require_email=true"
        )

    leads = []
    if broad.status_code == 200:
        payload = broad.json()
        if isinstance(payload, dict):
            leads = payload.get("leads", []) if isinstance(payload.get("leads", []), list) else []

    unique_emails: list[str] = []
    seen = set()
    for row in leads:
        for part in str(row.get("emails", "") or "").split(";"):
            email = part.strip().lower()
            if email and email not in seen:
                seen.add(email)
                unique_emails.append(email)

    checked = []
    checked_orgs = {}
    for row in leads:
        org = str(row.get("organization_name") or row.get("org_name") or "").strip()
        raw = str(row.get("emails", "") or "")
        for part in raw.split(";"):
            email = part.strip().lower()
            if email and email not in checked_orgs:
                checked_orgs[email] = org

    email_candidates = unique_emails[:20]

    for email in email_candidates:
        local, _, domain = email.partition("@")
        format_ok = bool(EMAIL_RE.match(email))
        role_like = local in ROLE_PREFIXES
        dns_ok = _domain_resolves(domain) if format_ok else False
        checked.append(
            {
                "org": checked_orgs.get(email, ""),
                "email": email,
                "format_ok": format_ok,
                "role_like": role_like,
                "dns_ok": dns_ok,
            }
        )

    trusted = [row for row in checked if row["format_ok"] and row["dns_ok"] and not row["role_like"]]
    risky = [row for row in checked if not (row["format_ok"] and row["dns_ok"]) or row["role_like"]]

    output = {
        "base": BASE,
        "login_status": login.status_code,
        "broad_status": broad.status_code,
        "leads_with_emails_seen": len(leads),
        "unique_emails": len(unique_emails),
        "emails_checked": len(checked),
        "trusted_candidates": len(trusted),
        "risky_candidates": len(risky),
        "trusted_samples": trusted[:20],
        "risky_samples": risky[:20],
    }
    print(json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    main()
