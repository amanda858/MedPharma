#!/usr/bin/env python3
"""Wait for Render to deploy commit 97946b8 then verify Eric is real and
visible to the chat picker. Hits /buildz (not /build-info)."""
import time
import urllib.request
import json
import sys

BASE = "https://medpharma-hub.onrender.com"
EXPECTED_PREFIX = "e8b2bbb"
LOGIN_URL = f"{BASE}/hub/api/login"
USERS_URL = f"{BASE}/hub/api/admin/users"
CHAT_URL = f"{BASE}/hub/api/chat/users"
BUILDZ_URL = f"{BASE}/buildz"


def get_json(url, cookie=None):
    req = urllib.request.Request(url)
    if cookie:
        req.add_header("Cookie", cookie)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def post_json(url, body, cookie=None):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if cookie:
        req.add_header("Cookie", cookie)
    r = urllib.request.urlopen(req, timeout=30)
    return r, json.loads(r.read().decode())


def wait_for_build():
    print(f"[wait] polling {BUILDZ_URL} for {EXPECTED_PREFIX}…")
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            d = get_json(BUILDZ_URL)
            marker = (d.get("build_marker") or "")[:7]
            print(f"  current={marker} want={EXPECTED_PREFIX}")
            if marker == EXPECTED_PREFIX:
                print("[wait] ✅ deploy is live")
                return True
        except Exception as e:
            print(f"  err: {e}")
        time.sleep(15)
    print("[wait] ❌ timeout")
    return False


def login_admin():
    r, _ = post_json(LOGIN_URL, {"username": "admin", "password": "admin123"})
    cookie = r.headers.get("Set-Cookie", "").split(";")[0]
    return cookie


def main():
    if not wait_for_build():
        sys.exit(1)
    cookie = login_admin()
    if not cookie:
        print("❌ admin login failed")
        sys.exit(1)
    print(f"[auth] admin cookie ok")

    users = get_json(USERS_URL, cookie)
    arr = users if isinstance(users, list) else users.get("users", [])
    print(f"\n--- /hub/api/admin/users ({len(arr)} users) ---")
    eric = None
    for u in arr:
        flag = ""
        if "eric" in (u.get("username") or "").lower():
            flag = "  <-- ERIC"
            eric = u
        print(f"  {u.get('username'):<35} role={u.get('role'):<8} active={u.get('is_active')}{flag}")

    chat = get_json(CHAT_URL, cookie)
    chat_arr = chat if isinstance(chat, list) else chat.get("users", [])
    print(f"\n--- /hub/api/chat/users ({len(chat_arr)} eligible chat users) ---")
    chat_eric = None
    for u in chat_arr:
        flag = ""
        if "eric" in (u.get("username") or "").lower():
            flag = "  <-- ERIC (visible in New Room picker)"
            chat_eric = u
        print(f"  {u.get('username'):<35} role={u.get('role'):<8}{flag}")

    print("\n=== RESULT ===")
    if eric and chat_eric:
        print("✅ PASS — Eric is in /admin/users AND /chat/users.")
        print(f"   username: {eric.get('username')}")
        print(f"   role:     {eric.get('role')}")
        print(f"   active:   {eric.get('is_active')}")
        print("   He will show up in the New Room member picker right now.")
    else:
        print(f"❌ FAIL — eric in admin/users={bool(eric)} chat/users={bool(chat_eric)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
