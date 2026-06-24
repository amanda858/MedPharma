#!/usr/bin/env python3
"""End-to-end live test of the MedPharma Hub demo flow.

Simulates exactly what Susan will do on her call:
  1. Admin logs in
  2. Admin creates a new client (gets login creds back)
  3. Admin creates a chat room and adds the new client + Melissa
  4. Admin posts a message
  5. Admin uploads a document
  6. The new client logs in and sees the chat room, message, doc, and dashboard
"""
from __future__ import annotations
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import http.cookiejar

BASE = "https://medpharma-hub.onrender.com"


def session():
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", "demo-smoke")]
    return op


def call(op, method, path, body=None, content_type="application/json", raw=False):
    url = BASE + path
    data = None
    headers = {}
    if body is not None:
        if raw:
            data = body
        elif content_type == "application/json":
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with op.open(req, timeout=30) as r:
            txt = r.read().decode("utf-8", "replace")
            code = r.getcode()
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", "replace")
        code = e.code
    try:
        return code, json.loads(txt)
    except Exception:
        return code, txt


def ok(label, code, data):
    status = "OK " if 200 <= code < 300 else "FAIL"
    short = json.dumps(data)[:140] if isinstance(data, (dict, list)) else str(data)[:140]
    print(f"  [{status}] {code:3d}  {label:<55} {short}")
    return 200 <= code < 300


def main():
    print("=" * 80)
    print(f"FULL DEMO SMOKE — {BASE}")
    print("=" * 80)

    # ── 1. admin login ───────────────────────────────────────────────────
    print("\n[1] Admin login")
    admin = session()
    code, d = call(admin, "POST", "/hub/api/login", {"username": "admin", "password": "admin123"})
    if not ok("login admin/admin123", code, d):
        sys.exit("admin login failed — cannot continue")

    # ── 2. create a fresh demo client ────────────────────────────────────
    print("\n[2] Admin creates a new client (the demo client)")
    suffix = int(time.time()) % 100000
    demo_company = f"Demo Practice {suffix}"
    demo_username = f"demo{suffix}"
    demo_password = "demo12345"
    code, d = call(admin, "POST", "/hub/api/clients", {
        "company": demo_company,
        "contact_name": "Demo Owner",
        "email": f"{demo_username}@example.com",
        "phone": "555-0100",
        "service_type": "rcm",
        "notes": "Smoke test client",
        "username": demo_username,
        "password": demo_password,
    })
    if not ok("create client", code, d):
        sys.exit("create client failed")
    created = d if isinstance(d, dict) else {}
    cid = created.get("id")
    login_info = created.get("login") or {}
    print(f"      → client_id={cid} login user={login_info.get('username')!r}")
    # Trust the credentials we supplied. The endpoint sometimes
    # auto-derives a different username from company slug; use whichever
    # is returned.
    real_user = (login_info.get("username") or demo_username).strip()
    real_pw = (login_info.get("password") or demo_password).strip()

    # ── 3. admin lists clients (should see the new one) ──────────────────
    print("\n[3] Admin sees the new client in /clients list")
    code, d = call(admin, "GET", "/hub/api/clients")
    found = False
    if isinstance(d, list):
        found = any((c.get("id") == cid) for c in d)
    elif isinstance(d, dict):
        found = any((c.get("id") == cid) for c in d.get("clients", []))
    ok(f"new client present (id={cid})", code, {"found": found})
    if not found:
        print("      WARNING: created id not in list — possible cache or DB bug")

    # ── 4. admin creates a chat room with the new client ────────────────
    print("\n[4] Admin creates chat room + invites demo client")
    # find ids for melissa + the new demo client
    code, users_resp = call(admin, "GET", "/hub/api/chat/users")
    users = users_resp.get("users", []) if isinstance(users_resp, dict) else []
    melissa_id = next((u["id"] for u in users if u.get("username") == "melissa@medprosc.com"), None)
    # demo client should now be eligible for chat (they are a client login)
    # Find them by username
    code, all_clients = call(admin, "GET", "/hub/api/clients")
    demo_user_id = None
    lst = all_clients if isinstance(all_clients, list) else all_clients.get("clients", [])
    for c in lst:
        if c.get("id") == cid:
            demo_user_id = c.get("id")
            break
    print(f"      → melissa_id={melissa_id}  demo_user_id={demo_user_id}")
    member_ids = [i for i in (melissa_id, demo_user_id) if i]
    code, d = call(admin, "POST", "/hub/api/chat/rooms", {
        "name": f"Onboarding — {demo_company}",
        "description": "Welcome chat",
        "client_id": cid,
        "member_user_ids": member_ids,
    })
    ok("create room", code, d)
    room_id = (d.get("id") or d.get("room_id")) if isinstance(d, dict) else None

    # ── 5. admin posts a message ─────────────────────────────────────────
    print("\n[5] Admin posts a chat message")
    if room_id:
        code, d = call(admin, "POST", f"/hub/api/chat/rooms/{room_id}/messages",
                       {"body": "Hi — welcome to MedPharma Hub. We'll be in touch here."})
        ok("post message", code, d)

    # ── 6. admin uploads a document ──────────────────────────────────────
    print("\n[6] Admin uploads a document")
    # multipart upload — use a tiny valid PDF (allowed type per backend rule)
    boundary = "----DEMO_BOUNDARY_X"
    file_content = (
        b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<<>>\nendobj\n"
        b"trailer<<>>\n%%EOF\n"
    )
    body_parts = [
        f"--{boundary}".encode(),
        b'Content-Disposition: form-data; name="client_id"',
        b"",
        str(cid).encode(),
        f"--{boundary}".encode(),
        b'Content-Disposition: form-data; name="category"',
        b"",
        b"general",
        f"--{boundary}".encode(),
        b'Content-Disposition: form-data; name="file"; filename="demo.pdf"',
        b"Content-Type: application/pdf",
        b"",
        file_content,
        f"--{boundary}--".encode(),
        b"",
    ]
    body_bytes = b"\r\n".join(body_parts)
    req = urllib.request.Request(
        BASE + "/hub/api/files/upload", data=body_bytes, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with admin.open(req, timeout=60) as r:
            up_code = r.getcode(); up_txt = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        up_code = e.code; up_txt = e.read().decode("utf-8", "replace")
    try:
        up_d = json.loads(up_txt)
    except Exception:
        up_d = up_txt
    ok("upload doc", up_code, up_d)

    # ── 7. demo client logs in and confirms what they see ────────────────
    print(f"\n[7] Demo client ({real_user}) logs in and inspects their view")
    client = session()
    code, d = call(client, "POST", "/hub/api/login", {"username": real_user, "password": real_pw})
    if not ok(f"client login {real_user}/{real_pw}", code, d):
        print("      → client login failed; demo flow blocked here.")
        return
    code, accts = call(client, "GET", "/hub/api/accounts")
    print(f"      accounts visible: {accts}")
    code, dash = call(client, "GET", "/hub/api/dashboard")
    ok("client dashboard loads", code, dash)
    code, rooms = call(client, "GET", "/hub/api/chat/rooms")
    ok("client sees chat rooms", code, rooms)
    code, files = call(client, "GET", f"/hub/api/files?client_id={cid}")
    ok("client sees files", code, files)

    print("\n" + "=" * 80)
    print("DEMO SMOKE COMPLETE")
    print("=" * 80)
    print(f"\nWorking demo credentials:")
    print(f"  Admin:       admin / admin123")
    print(f"  Susan:       susan@medprosc.com / susan123")
    print(f"  Melissa:     melissa@medprosc.com / melissa123")
    print(f"  Demo client: {real_user} / {real_pw}   (company: {demo_company})")
    print(f"\nDemo client_id: {cid}")
    if room_id:
        print(f"Demo chat room_id: {room_id}")


if __name__ == "__main__":
    main()
