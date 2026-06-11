"""Wait for the new build to deploy, then force-reset susan/melissa passwords
via the new admin endpoint, then verify login works."""

import http.cookiejar
import json
import time
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"
TARGET_COMMIT_PREFIX = "c6abc8e"

ACCOUNTS = [
    ("susan@medprosc.com", "susan123"),
    ("melissa@medprosc.com", "melissa123"),
]

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return -1, {"error": str(e)}


def post_json(url, data, timeout=30):
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with opener.open(req, timeout=timeout) as r:
            body = r.read().decode()
            try:
                return r.status, json.loads(body)
            except Exception:
                return r.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body


def wait_for_build(prefix: str, max_wait_s: int = 300) -> bool:
    deadline = time.time() + max_wait_s
    last = None
    while time.time() < deadline:
        code, body = get_json(f"{BASE}/buildz")
        marker = (body or {}).get("build_marker", "") if isinstance(body, dict) else ""
        if marker != last:
            print(f"  build_marker = {marker[:12]}  (waiting for {prefix})")
            last = marker
        if marker.startswith(prefix):
            return True
        time.sleep(8)
    return False


def main() -> int:
    print(f"=== Waiting for deploy {TARGET_COMMIT_PREFIX} to go live ===")
    if not wait_for_build(TARGET_COMMIT_PREFIX):
        print(f"!! Deploy did not reach {TARGET_COMMIT_PREFIX} within 5 min — check Render dashboard")
        return 1
    print(f"✓ Deploy {TARGET_COMMIT_PREFIX} is live\n")

    # 1) Login as rcm (admin)
    print("=== Logging in as rcm ===")
    code, body = post_json(f"{BASE}/hub/api/login", {"username": "rcm", "password": "rcm123"})
    print(f"  login -> HTTP {code}")
    if code != 200:
        print(f"  body: {body}")
        return 2

    # 2) Force-reset each account
    print("\n=== Force-resetting passwords ===")
    for username, password in ACCOUNTS:
        code, body = post_json(
            f"{BASE}/hub/api/admin/users/force-password",
            {"username": username, "new_password": password},
        )
        print(f"  {username} -> HTTP {code}  {body}")

    # 3) Verify login on each account in a fresh cookie jar
    print("\n=== Verifying logins ===")
    for username, password in ACCOUNTS:
        fresh_cj = http.cookiejar.CookieJar()
        fresh_opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(fresh_cj)
        )
        req = urllib.request.Request(
            f"{BASE}/hub/api/login",
            data=json.dumps({"username": username, "password": password}).encode(),
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with fresh_opener.open(req, timeout=30) as r:
                print(f"  {username:30s} -> HTTP {r.status}  ✓ LOGIN OK")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  {username:30s} -> HTTP {e.code}  ✗ FAIL: {body[:200]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
