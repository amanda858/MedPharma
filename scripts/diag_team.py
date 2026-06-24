#!/usr/bin/env python3
"""Wait for 485a47d deploy then call /admin/diag/users and
/admin/diag/ensure-team to figure out why Eric and rcm@medprosc.com
are missing."""
import time
import urllib.request
import json
import sys

BASE = "https://medpharma-hub.onrender.com"
EXPECTED = "10b7670"


def get_json(url, cookie=None, method="GET"):
    req = urllib.request.Request(url, method=method)
    if cookie:
        req.add_header("Cookie", cookie)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.headers, json.loads(r.read().decode())


def post_json(url, body, cookie=None):
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if cookie:
        req.add_header("Cookie", cookie)
    r = urllib.request.urlopen(req, timeout=30)
    return r, json.loads(r.read().decode())


def wait_for_build():
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            _, d = get_json(f"{BASE}/buildz")
            marker = (d.get("build_marker") or "")[:7]
            print(f"  current={marker} want={EXPECTED}")
            if marker == EXPECTED:
                return True
        except Exception as e:
            print(f"  err: {e}")
        time.sleep(15)
    return False


def main():
    print(f"[wait] polling for {EXPECTED}…")
    if not wait_for_build():
        sys.exit(1)
    print("[wait] ✅ deploy is live\n")

    r, _ = post_json(f"{BASE}/hub/api/login", {"username": "admin", "password": "admin123"})
    cookie = r.headers.get("Set-Cookie", "").split(";")[0]
    print(f"[auth] cookie ok\n")

    # 1) Snapshot DB
    print("=== /admin/diag/users (BEFORE) ===")
    _, d = get_json(f"{BASE}/hub/api/admin/diag/users", cookie=cookie)
    print(f"user_count={d['user_count']}")
    for u in d["users"]:
        print(f"  id={u['id']:<3} {u['username']:<35} role={u['role']:<8} active={u['is_active']} co={u['company']}")
    print(f"\nmigrations ({len(d['migrations'])}):")
    for m in d["migrations"]:
        print(f"  {m}")

    # 2) Force ensure
    print("\n=== POST /admin/diag/ensure-team ===")
    _, d2 = post_json(f"{BASE}/hub/api/admin/diag/ensure-team", None, cookie=cookie)
    print(json.dumps(d2, indent=2))

    # 3) After
    print("\n=== /admin/diag/users (AFTER) ===")
    _, d3 = get_json(f"{BASE}/hub/api/admin/diag/users", cookie=cookie)
    print(f"user_count={d3['user_count']}")
    for u in d3["users"]:
        marker = "  <-- ERIC" if "eric" in u["username"].lower() else ""
        print(f"  id={u['id']:<3} {u['username']:<35} role={u['role']:<8} active={u['is_active']}{marker}")


if __name__ == "__main__":
    main()
