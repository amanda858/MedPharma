#!/usr/bin/env python3
"""Probe /hub/api/accounts on live."""
import json
import urllib.request
import urllib.error
import http.cookiejar

BASE = "https://medpharma-hub.onrender.com"

def main():
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    # Login
    print("=== POST /hub/api/login ===")
    data = json.dumps({"username": "admin", "password": "admin123"}).encode()
    req = urllib.request.Request(
        f"{BASE}/hub/api/login",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        r = opener.open(req, timeout=60)
        body = r.read().decode("utf-8", "replace")
        print("status:", r.status)
        print("body:", body[:400])
    except urllib.error.HTTPError as e:
        print("HTTPError", e.code)
        print(e.read().decode("utf-8", "replace")[:400])
        return

    # /hub/api/me
    print("\n=== GET /hub/api/me ===")
    try:
        r = opener.open(f"{BASE}/hub/api/me", timeout=60)
        print("status:", r.status)
        print("body:", r.read().decode("utf-8", "replace")[:300])
    except urllib.error.HTTPError as e:
        print("HTTPError", e.code)
        print(e.read().decode("utf-8", "replace")[:400])

    # /hub/api/accounts
    print("\n=== GET /hub/api/accounts ===")
    try:
        r = opener.open(f"{BASE}/hub/api/accounts", timeout=60)
        body = r.read().decode("utf-8", "replace")
        print("status:", r.status)
        print("body length:", len(body))
        try:
            data = json.loads(body)
            print("type:", type(data).__name__)
            if isinstance(data, list):
                print("count:", len(data))
                for c in data[:5]:
                    print("  -", c.get("id"), c.get("company"), c.get("role"))
            else:
                print("body:", body[:500])
        except Exception:
            print("body:", body[:500])
    except urllib.error.HTTPError as e:
        print("HTTPError", e.code)
        print(e.read().decode("utf-8", "replace")[:600])
    except Exception as e:
        print("Error:", type(e).__name__, e)


if __name__ == "__main__":
    main()
