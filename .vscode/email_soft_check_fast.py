import json
import os
import re
import socket
from urllib import request, parse
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse

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
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def _http_json(url: str, method: str = "GET", body: dict | None = None, cookie: str = ""):
    data = None
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method=method)
    with request.urlopen(req, timeout=25) as resp:
        raw = resp.read().decode("utf-8")
        set_cookie = resp.headers.get("Set-Cookie", "")
        return resp.status, json.loads(raw), set_cookie


def main():
    out = {"base": BASE}
    try:
        status, buildz, _ = _http_json(f"{BASE}/buildz")
        out["buildz_status"] = status
        out["build_marker"] = buildz.get("build_marker")

        _, _, set_cookie = _http_json(
            f"{BASE}/hub/api/login",
            method="POST",
            body={"username": "admin", "password": "admin123"},
        )
        cookie = set_cookie.split(";", 1)[0] if set_cookie else ""
        out["auth_cookie"] = bool(cookie)

        strict_url = (
            f"{BASE}/admin/leads/api/leads?quality_only=true&need_signal_only=true"
            f"&require_email=true&quality_tier=strict"
        )
        direct_url = (
            f"{BASE}/admin/leads/api/leads?quality_only=true&need_signal_only=true"
            f"&need_signal_source=direct&require_email=true&quality_tier=strict"
        )

        strict_status, strict_json, _ = _http_json(strict_url, cookie=cookie)
        direct_status, direct_json, _ = _http_json(direct_url, cookie=cookie)

        out["strict_status"] = strict_status
        out["direct_status"] = direct_status
        strict_leads = strict_json.get("leads", []) if isinstance(strict_json, dict) else []
        direct_leads = direct_json.get("leads", []) if isinstance(direct_json, dict) else []
        out["strict_count"] = strict_json.get("count", len(strict_leads)) if isinstance(strict_json, dict) else 0
        out["direct_strict_count"] = direct_json.get("count", len(direct_leads)) if isinstance(direct_json, dict) else 0

        checked = 0
        format_ok = 0
        domains = set()
        sample = []
        for row in strict_leads[:20]:
            org = row.get("organization_name") or row.get("org_name") or ""
            raw_emails = str(row.get("emails") or "")
            emails = [e.strip() for e in raw_emails.split(";") if e.strip()]
            good = [e for e in emails if EMAIL_RE.match(e)]
            checked += len(emails)
            format_ok += len(good)
            for e in good:
                domains.add(e.split("@", 1)[1].lower())
            sample.append({"org": org, "emails": good[:3], "notes": str(row.get("notes", ""))[:100]})

        resolvable = 0
        unresolved = []
        for d in list(domains)[:30]:
            try:
                socket.getaddrinfo(d, 443)
                resolvable += 1
            except Exception:
                unresolved.append(d)

        out["email_soft_check"] = {
            "emails_checked": checked,
            "format_valid": format_ok,
            "format_valid_pct": round((format_ok / checked * 100.0), 1) if checked else 0.0,
            "domains_checked": min(len(domains), 30),
            "domains_resolvable": resolvable,
            "domains_unresolved_count": len(unresolved),
            "domains_unresolved_sample": unresolved[:10],
            "strict_sample": sample[:8],
        }

    except (URLError, HTTPError, TimeoutError) as exc:
        out["error"] = str(exc)
    except Exception as exc:
        out["error"] = str(exc)

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
