"""Watch Render until SENDGRID_API_KEY is set and a real email actually sends.

Polls /hub/api/admin/diag/email every 15s for up to 10 minutes. As soon as
the provider flips to ready, fires a demo EOD email and reports the result.
Run it AFTER pasting SENDGRID_API_KEY into the Render dashboard.
"""
from __future__ import annotations

import http.cookiejar
import json
import time
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"
MAX_WAIT_SECONDS = 600
POLL_EVERY = 15


def make_session():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def call(opener, method, path, payload=None, timeout=30):
    url = f"{BASE}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method=method,
    )
    try:
        r = opener.open(req, timeout=timeout)
        body = r.read().decode("utf-8", "ignore")
        try:
            return r.getcode(), json.loads(body)
        except Exception:
            return r.getcode(), body
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = (e.read() or b"").decode("utf-8", "ignore")
        except Exception:
            pass
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"


def login_admin():
    s = make_session()
    code, _ = call(s, "POST", "/hub/api/login",
                   {"username": "admin", "password": "admin123"})
    if code != 200:
        raise SystemExit(f"admin login failed: {code}")
    return s


def main() -> int:
    print(f"\nWatching {BASE} for SENDGRID_API_KEY to land…")
    print(f"(polling every {POLL_EVERY}s, up to {MAX_WAIT_SECONDS // 60} min)\n")

    s = login_admin()
    started = time.time()
    last_marker = ""
    while time.time() - started < MAX_WAIT_SECONDS:
        # Re-login if cookie expired between deploys
        code, body = call(s, "GET", "/hub/api/admin/diag/email")
        if code in (401, 403):
            s = login_admin()
            code, body = call(s, "GET", "/hub/api/admin/diag/email")

        if not isinstance(body, dict):
            print(f"  diag returned non-json (http {code}); retrying…")
            time.sleep(POLL_EVERY)
            continue

        em = body.get("email", {})
        marker = (
            f"key={bool(em.get('sendgrid_key_set'))} "
            f"prefix={em.get('sendgrid_key_prefix') or '-'} "
            f"from={em.get('sendgrid_from')} "
            f"smtp={bool(em.get('smtp_host_set'))} "
            f"ready={em.get('ready')}"
        )
        if marker != last_marker:
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] {marker}")
            last_marker = marker

        if em.get("ready"):
            print("\n✅ provider ready — firing demo EOD email…\n")
            code, body = call(s, "POST",
                              "/hub/api/admin/reports/eod/send-now?demo=true",
                              timeout=120)
            if isinstance(body, dict):
                sent = body.get("sent", [])
                failed = body.get("failed", [])
                if sent and not failed:
                    print(f"✅ DELIVERED — sent={len(sent)} → {[s.get('email') for s in sent]}")
                    print("   Check lexi@medprosc.com inbox.")
                    return 0
                print(f"❌ provider says ready but send failed: sent={len(sent)} failed={len(failed)}")
                if failed:
                    print(f"   reason: {failed[0]}")
                return 1
            print(f"❌ unexpected response: {body!r}")
            return 1

        time.sleep(POLL_EVERY)

    print(f"\n⏱  Timed out after {MAX_WAIT_SECONDS}s — env var never flipped to ready.")
    print("   Double-check Render dashboard → Environment → SENDGRID_API_KEY actually saved.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
