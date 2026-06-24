import json
import os
import socket
from urllib.parse import urlparse

import httpx

PRIMARY_BASE = os.getenv("LEADS_BASE_URL", "https://medpharmahub.com").rstrip("/")
FALLBACK_BASE = "https://medpharma-hub.onrender.com"
USERNAME = os.getenv("HUB_ADMIN_USERNAME", "admin")
PASSWORD = os.getenv("HUB_ADMIN_PASSWORD", "admin123")


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

try:
    with httpx.Client(timeout=30, follow_redirects=True) as c:
        login = c.post(
            f"{BASE}/hub/api/login",
            json={"username": USERNAME, "password": PASSWORD},
        )
        out["login_status"] = login.status_code
        if login.status_code != 200:
            out["error"] = "login_failed"
        else:
            relink = c.post(
                f"{BASE}/hub/api/admin/production/relink-kindercare",
                json={
                    "source_client_ids": [4],
                    "usernames": ["Mike", "Sarah"],
                    "dry_run": False,
                    "max_rows": 5000,
                },
            )
            out["relink_status"] = relink.status_code
            try:
                out["relink"] = relink.json()
            except Exception:
                out["relink_raw"] = relink.text[:500]
except Exception as exc:
    out["error"] = str(exc)

print(json.dumps(out, ensure_ascii=True))
