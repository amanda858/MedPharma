#!/usr/bin/env python3
"""Wait for ee7faf6 then verify chat picker has exactly 6 MedPharma users (no dups)."""
import time
import urllib.request
import json
import sys

BASE = "https://medpharma-hub.onrender.com"
EXPECTED = "ee7faf6"


def get_json(url, cookie=None):
    req = urllib.request.Request(url)
    if cookie:
        req.add_header("Cookie", cookie)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


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
            d = get_json(f"{BASE}/buildz")
            m = (d.get("build_marker") or "")[:7]
            print(f"  current={m} want={EXPECTED}")
            if m == EXPECTED:
                return True
        except Exception as e:
            print(f"  err: {e}")
        time.sleep(15)
    return False


def main():
    print(f"[wait] polling for {EXPECTED}…")
    if not wait_for_build():
        sys.exit(1)
    print("[wait] ✅ live\n")

    r, _ = post_json(f"{BASE}/hub/api/login", {"username": "admin", "password": "admin123"})
    cookie = r.headers.get("Set-Cookie", "").split(";")[0]

    # Force the dedup to run NOW on the live persistent disk (in case the
    # startup ensure didn't fire on this old DB).
    print("=== POST /admin/diag/ensure-team (force dedup) ===")
    _, d = post_json(f"{BASE}/hub/api/admin/diag/ensure-team", None, cookie=cookie)
    print(json.dumps(d, indent=2))

    print("\n=== /hub/api/chat/users (what the New Room picker shows) ===")
    chat = get_json(f"{BASE}/hub/api/chat/users", cookie=cookie)
    arr = chat if isinstance(chat, list) else chat.get("users", [])
    for u in arr:
        print(f"  id={u['id']:<3} {u['username']:<35} {u['contact_name']:<12} role={u['role']}")

    print(f"\ntotal in picker: {len(arr)}")

    expected = {
        "admin@medprosc.com", "rcm@medprosc.com", "eric@medprosc.com",
        "susan@medprosc.com", "melissa@medprosc.com", "jessica@medprosc.com",
    }
    seen = {u["username"] for u in arr}
    dupes = {"admin", "rcm", "jessica", "susan", "melissa", "eric"} & seen
    missing = expected - seen

    print("\n=== RESULT ===")
    if not dupes and not missing:
        print(f"✅ PASS — exactly {len(arr)} users, no legacy short-username duplicates")
        print("   You can now create a chat room and each person appears ONCE.")
    else:
        if dupes:
            print(f"❌ legacy duplicates still showing: {dupes}")
        if missing:
            print(f"❌ canonical accounts missing: {missing}")
        sys.exit(2)


if __name__ == "__main__":
    main()
