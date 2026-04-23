import json
import os
import re
import socket
import time
from pathlib import Path
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

out = {"base": BASE}

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


def _soft_email_audit(leads: list[dict], cap: int = 120) -> dict:
    unique_emails: list[str] = []
    seen = set()
    for row in leads:
        raw = str(row.get("emails", "") or "")
        for part in raw.split(";"):
            email = part.strip().lower()
            if email and email not in seen:
                seen.add(email)
                unique_emails.append(email)

    checked = []
    for email in unique_emails[:cap]:
        local, _, domain = email.partition("@")
        format_ok = bool(EMAIL_RE.match(email))
        role_like = local in ROLE_PREFIXES
        dns_ok = _domain_resolves(domain) if format_ok else False
        checked.append({
            "email": email,
            "format_ok": format_ok,
            "role_like": role_like,
            "dns_ok": dns_ok,
        })

    trusted = [item for item in checked if item["format_ok"] and item["dns_ok"] and not item["role_like"]]
    risky = [item for item in checked if not (item["format_ok"] and item["dns_ok"]) or item["role_like"]]
    return {
        "unique_emails": len(unique_emails),
        "checked": len(checked),
        "trusted_candidates": len(trusted),
        "risky_candidates": len(risky),
        "trusted_samples": trusted[:8],
        "risky_samples": risky[:8],
    }


def _fetch_counts(client: httpx.Client) -> dict:
    strict = client.get(
        f"{BASE}/admin/leads/api/leads?quality_only=true&need_signal_only=true&require_email=true"
    )
    broad = client.get(
        f"{BASE}/admin/leads/api/leads?quality_only=false&need_signal_only=false&require_email=false"
    )
    data = {
        "strict_status": strict.status_code,
        "broad_status": broad.status_code,
    }
    if strict.status_code == 200:
        s = strict.json()
        data["strict_count"] = s.get("count", len(s.get("leads", [])))
        data["strict_sample"] = [
            {
                "org": row.get("organization_name") or row.get("org_name"),
                "emails": row.get("emails"),
                "notes": str(row.get("notes", ""))[:110],
            }
            for row in s.get("leads", [])[:3]
        ]
    if broad.status_code == 200:
        b = broad.json()
        data["broad_count"] = b.get("count", len(b.get("leads", [])))
    return data


def _fetch_direct_need_counts(client: httpx.Client) -> dict:
    strict = client.get(
        f"{BASE}/admin/leads/api/leads?quality_only=true&need_signal_only=true&need_signal_source=direct&require_email=true&quality_tier=strict"
    )
    review = client.get(
        f"{BASE}/admin/leads/api/leads?quality_only=true&need_signal_only=true&need_signal_source=direct&require_email=true&quality_tier=review"
    )

    data = {
        "strict_status": strict.status_code,
        "review_status": review.status_code,
    }
    if strict.status_code == 200:
        s = strict.json()
        data["strict_count"] = s.get("count", len(s.get("leads", [])))
        strict_leads = s.get("leads", []) if isinstance(s.get("leads", []), list) else []
        data["strict_sample"] = [
            {
                "org": row.get("organization_name") or row.get("org_name"),
                "emails": row.get("emails"),
                "notes": str(row.get("notes", ""))[:110],
                "tags": str(row.get("tags", ""))[:110],
            }
            for row in strict_leads[:3]
        ]
        data["email_soft_audit"] = _soft_email_audit(strict_leads)
    if review.status_code == 200:
        r = review.json()
        data["review_count"] = r.get("count", len(r.get("leads", [])))
    return data


def _fetch_counts_with_fresh_auth() -> dict:
    with httpx.Client(timeout=30, follow_redirects=True) as c2:
        c2.post(
            f"{BASE}/hub/api/login",
            json={"username": "admin", "password": "admin123"},
        )
        return _fetch_counts(c2)

try:
    with httpx.Client(timeout=30, follow_redirects=True) as c:
        out["healthz"] = c.get(f"{BASE}/healthz").status_code
        out["hub_buildz"] = c.get(f"{BASE}/buildz").json()

        login = c.post(
            f"{BASE}/hub/api/login",
            json={"username": "admin", "password": "admin123"},
        )
        out["login_status"] = login.status_code

        leads_buildz = c.get(f"{BASE}/admin/leads/buildz")
        out["leads_buildz_status"] = leads_buildz.status_code
        try:
            out["leads_buildz"] = leads_buildz.json()
        except Exception:
            out["leads_buildz_raw"] = leads_buildz.text[:180]

        ui = c.get(f"{BASE}/admin/leads/")
        text = ui.text
        out["ui_status"] = ui.status_code
        out["ui_markers"] = {
            "show_exploratory_toggle": "showExploratoryLeads" in text,
            "strict_summary": "Showing strict actionable leads" in text,
            "fast_poll_path": "poll-daily?segment=all&fast=true" in text,
        }

        out["pre_poll"] = _fetch_counts(c)
        out["pre_poll_direct_need"] = _fetch_direct_need_counts(c)

        # Emergency recovery: repopulate missing emails for saved leads.
        enrich_emails = c.post(f"{BASE}/admin/leads/api/admin/enrich-emails")
        out["enrich_emails_status"] = enrich_emails.status_code
        out["enrich_emails_body"] = (
            enrich_emails.json()
            if enrich_emails.headers.get("content-type", "").startswith("application/json")
            else enrich_emails.text[:240]
        )

        poll = c.post(f"{BASE}/admin/leads/api/leads/poll-daily?segment=all&fast=true")
        out["poll_start_status"] = poll.status_code
        out["poll_start_body"] = poll.json() if poll.headers.get("content-type", "").startswith("application/json") else poll.text

        timeout_s = 210
        interval_s = 6
        deadline = time.time() + timeout_s
        checks = 0
        final_status = None
        while time.time() < deadline:
            checks += 1
            c.post(
                f"{BASE}/hub/api/login",
                json={"username": "admin", "password": "admin123"},
            )
            poll_status = c.get(f"{BASE}/admin/leads/api/leads/poll-status")
            if poll_status.status_code != 200:
                final_status = {"status_code": poll_status.status_code, "body": poll_status.text[:200]}
                time.sleep(interval_s)
                continue

            status_json = poll_status.json()
            final_status = status_json
            running = bool((status_json.get("status") or {}).get("running"))
            if not running:
                break
            time.sleep(interval_s)

        out["poll_wait_checks"] = checks
        out["poll_wait_timeout"] = bool((final_status or {}).get("status", {}).get("running"))
        out["poll_status"] = final_status

        # Session can expire during long poll waits; refresh auth before final reads.
        c.post(
            f"{BASE}/hub/api/login",
            json={"username": "admin", "password": "admin123"},
        )
        out["post_poll"] = _fetch_counts(c)
        out["post_poll_direct_need"] = _fetch_direct_need_counts(c)
        if int((out.get("post_poll") or {}).get("strict_status", 0) or 0) == 401:
            out["post_poll"] = _fetch_counts_with_fresh_auth()
            out["post_poll_direct_need"] = _fetch_direct_need_counts(c)

        # If polling is still running, this snapshot is transitional and should
        # not be treated as a strict-pool failure signal.
        if bool(out.get("poll_wait_timeout")):
            out["warning"] = {
                "code": "POLL_STILL_RUNNING",
                "message": "Poll still running at timeout; post_poll counts are transitional",
            }

        pre_strict = int((out.get("pre_poll") or {}).get("strict_count", 0) or 0)
        post_strict = int((out.get("post_poll") or {}).get("strict_count", 0) or 0)
        out["strict_delta"] = post_strict - pre_strict
        if post_strict == 0 and not bool(out.get("poll_wait_timeout")):
            out["alert"] = {
                "code": "STRICT_ZERO",
                "message": "Strict actionable leads are still zero after poll verification",
            }
except Exception as e:
    out["error"] = str(e)

try:
    Path(".vscode/live_check_latest.json").write_text(
        json.dumps(out, ensure_ascii=False),
        encoding="utf-8",
    )
except Exception:
    pass

print(json.dumps(out, ensure_ascii=False))
