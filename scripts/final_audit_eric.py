#!/usr/bin/env python3
"""FINAL end-to-end audit for Eric, the team-count line, and chat picker."""
import time
import urllib.request
import json
import sys

BASE = "https://medpharma-hub.onrender.com"
EXPECTED = "d79641e"


def get_json(url, cookie=None):
    req = urllib.request.Request(url)
    if cookie:
        req.add_header("Cookie", cookie)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def get_text(url, cookie=None):
    req = urllib.request.Request(url)
    if cookie:
        req.add_header("Cookie", cookie)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode()


def post_json(url, body, cookie=None):
    data = json.dumps(body).encode()
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
            marker = (d.get("build_marker") or "")[:7]
            print(f"  current={marker} want={EXPECTED}")
            if marker == EXPECTED:
                return True
        except Exception as e:
            print(f"  err: {e}")
        time.sleep(15)
    return False


def check(label, condition, detail=""):
    icon = "✅" if condition else "❌"
    print(f"  {icon} {label}{(' — ' + detail) if detail else ''}")
    return condition


def main():
    print(f"[wait] polling for {EXPECTED}…")
    if not wait_for_build():
        print("❌ deploy didn't ship in time")
        sys.exit(1)
    print("[wait] ✅ deploy live\n")

    results = []

    # 1) Hub HTML loads
    print("== 1) Hub HTML ==")
    html = get_text(f"{BASE}/hub")
    results.append(check("hub page loads", len(html) > 1000, f"{len(html)} bytes"))
    results.append(check("team-count line element present",
                         "selectorTeamLine" in html))
    results.append(check("View / Manage Team button wired",
                         "enterAsAdminAndOpenClients" in html))
    results.append(check("refreshSelectorTeamLine defined",
                         "refreshSelectorTeamLine" in html))

    # 2) Eric login
    print("\n== 2) Eric login ==")
    try:
        r, d = post_json(
            f"{BASE}/hub/api/login",
            {"username": "eric@medprosc.com", "password": "eric123"},
        )
        eric_cookie = r.headers.get("Set-Cookie", "").split(";")[0]
        results.append(check("eric@medprosc.com / eric123 login", d.get("ok") is True,
                             f"role={d.get('user',{}).get('role')}"))
    except Exception as e:
        results.append(check("eric@medprosc.com login", False, str(e)))
        eric_cookie = None

    # 3) Admin login + admin endpoints
    print("\n== 3) Admin perspective ==")
    r, _ = post_json(
        f"{BASE}/hub/api/login",
        {"username": "admin", "password": "admin123"},
    )
    cookie = r.headers.get("Set-Cookie", "").split(";")[0]
    results.append(check("admin login", bool(cookie)))

    admin_users = get_json(f"{BASE}/hub/api/admin/users", cookie=cookie)
    arr = admin_users if isinstance(admin_users, list) else admin_users.get("users", [])
    eric_admin = [u for u in arr if u["username"] == "eric@medprosc.com"]
    results.append(check("Eric in /admin/users (team mgmt list)",
                         len(eric_admin) == 1,
                         f"{eric_admin[0]['contact_name']}, role={eric_admin[0]['role']}" if eric_admin else "missing"))

    chat = get_json(f"{BASE}/hub/api/chat/users", cookie=cookie)
    chat_arr = chat if isinstance(chat, list) else chat.get("users", [])
    eric_chat = [u for u in chat_arr if u["username"] == "eric@medprosc.com"]
    results.append(check("Eric in /chat/users (New Room picker)",
                         len(eric_chat) == 1,
                         f"role={eric_chat[0]['role']}" if eric_chat else "missing"))

    results.append(check(f"total chat-eligible users >= 6",
                         len(chat_arr) >= 6, f"{len(chat_arr)} users"))

    # 4) Eric's chat perspective
    if eric_cookie:
        print("\n== 4) Eric's perspective (staff/admin) ==")
        try:
            eric_chat = get_json(f"{BASE}/hub/api/chat/users", cookie=eric_cookie)
            eric_chat_arr = eric_chat if isinstance(eric_chat, list) else eric_chat.get("users", [])
            results.append(check("Eric can list chat users",
                                 len(eric_chat_arr) > 0,
                                 f"{len(eric_chat_arr)} users"))
        except Exception as e:
            results.append(check("Eric can list chat users", False, str(e)))

    passed = sum(1 for r in results if r)
    total = len(results)
    print(f"\n=== {passed}/{total} checks PASS ===")
    if passed != total:
        sys.exit(2)


if __name__ == "__main__":
    main()
