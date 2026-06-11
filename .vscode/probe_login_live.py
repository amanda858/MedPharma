"""Probe live Render login endpoint for susan/melissa."""

import http.cookiejar
import json
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"


def hit(username: str, password: str) -> None:
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    body = json.dumps({"username": username, "password": password}).encode()
    req = urllib.request.Request(
        f"{BASE}/hub/api/login",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with opener.open(req, timeout=30) as resp:
            print(f"[{username}] HTTP {resp.status}")
            print(resp.read().decode("utf-8", errors="replace")[:500])
    except urllib.error.HTTPError as exc:
        print(f"[{username}] HTTP {exc.code}")
        print(exc.read().decode("utf-8", errors="replace")[:500])
    except Exception as exc:
        print(f"[{username}] ERR {exc}")


def main() -> None:
    # Confirm the service is up
    try:
        with urllib.request.urlopen(f"{BASE}/healthz", timeout=15) as resp:
            print(f"[healthz] HTTP {resp.status}: {resp.read().decode()[:200]}")
    except Exception as exc:
        print(f"[healthz] ERR {exc}")

    # Check which build is live
    try:
        with urllib.request.urlopen(f"{BASE}/buildz", timeout=15) as resp:
            print(f"[buildz]  HTTP {resp.status}: {resp.read().decode()[:300]}")
    except Exception as exc:
        print(f"[buildz]  ERR {exc}")

    # Try every plausible combo so we can see which one the live DB actually has.
    for u, p in [
        ("susan@medprosc.com", "susan123"),
        ("susan", "susan123"),
        ("melissa@medprosc.com", "melissa123"),
        ("melissa", "melissa123"),
        ("jessica", "jessica123"),
        ("rcm", "rcm123"),
    ]:
        hit(u, p)


if __name__ == "__main__":
    main()
