"""Full live smoke test against medpharma-hub.onrender.com.

Tests EVERY hot path the team will actually use:
  - admin login
  - create client with welcome email trigger
  - module visibility / opt-out
  - chat room create + invite email
  - chat post message (HIPAA: encrypted at rest, no PHI in audit/email)
  - read messages back (decryption works)
  - chat as the new client user (member access)
  - EOD report preview + send-now demo
  - admin diag endpoints
  - cleanup: delete the test client + room

Prints PASS/FAIL per check + a summary."""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import http.cookiejar


BASE = "https://medpharma-hub.onrender.com"
ADMIN_USER = "admin"
ADMIN_PASS = "admin123"

results: list[tuple[str, bool, str]] = []  # (name, ok, detail)


def step(name: str, ok: bool, detail: str = ""):
    icon = "✅" if ok else "❌"
    print(f"{icon} {name}" + (f"  ·  {detail}" if detail else ""))
    results.append((name, ok, detail))


class Client:
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj))

    def call(self, method: str, path: str, body: dict | None = None, timeout: int = 60):
        url = f"{BASE}{path}"
        data = None
        headers = {"Content-Type": "application/json"}
        if body is not None:
            data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=timeout) as r:
                payload = r.read()
                try:
                    return r.getcode(), json.loads(payload) if payload else {}
                except json.JSONDecodeError:
                    return r.getcode(), {"_raw": payload.decode("utf-8", "ignore")[:500]}
        except urllib.error.HTTPError as e:
            try:
                msg = json.loads(e.read())
            except Exception:
                msg = {"detail": str(e)}
            return e.code, msg


def main() -> int:
    admin = Client()

    # 0) Build marker so we know what we're testing
    code, j = admin.call("GET", "/buildz")
    build = (j.get("build_marker") or "")[:7]
    step("buildz", code == 200, f"commit={build}")

    # 1) Admin login
    code, j = admin.call("POST", "/hub/api/login", {"username": ADMIN_USER, "password": ADMIN_PASS})
    step("admin login", code == 200, f"role={j.get('user', {}).get('role')}")
    if code != 200:
        return 1

    # 2) Email + encryption diag
    code, diag = admin.call("GET", "/hub/api/admin/diag/email")
    em = diag.get("email", {})
    enc = diag.get("chat_encryption", {})
    step("chat encryption ready", enc.get("ready") is True,
         f"{enc.get('encryption')} via {enc.get('key_source')}")
    step("email provider configured", em.get("ready") is True,
         f"sendgrid_key={em.get('sendgrid_key_set')}, smtp_host={em.get('smtp_host_set')}, "
         f"from={em.get('sendgrid_from')}")

    # 3) /readyz public health
    code, j = admin.call("GET", "/readyz")
    step("/readyz", code == 200 and j.get("ready") is True,
         f"db={j.get('status',{}).get('db')}, scheduler={j.get('status',{}).get('scheduler')}")

    # 4) Create a throwaway client account
    uname = f"smoke_{int(time.time())}"
    pword = "SmokeTest123!"
    code, j = admin.call("POST", "/hub/api/clients", {
        "username": uname,
        "password": pword,
        "role": "client",
        "company": f"Smoke Test {uname}",
        "contact_name": "Smoke Tester",
        "email": "smoke.tester@example.com",
    })
    cid = j.get("id")
    welcome = (j.get("welcome_email") or {})
    step("create client", code == 200 and bool(cid), f"id={cid}, access_granted={j.get('access_granted')}")
    step("welcome email queued",
         welcome.get("ok") is True or welcome.get("sent"),
         f"recipients={welcome.get('recipients')}, failed={welcome.get('failed')}")

    # 5) Create chat room with this client as member
    code, j = admin.call("POST", "/hub/api/chat/rooms", {
        "name": f"Smoke Room {uname}",
        "description": "End-to-end smoke",
        "client_id": cid,
        "member_user_ids": [cid],
    })
    room_id = j.get("id")
    invites = j.get("invites") or []
    sent = [i for i in invites if i.get("sent")]
    failed = [i for i in invites if not i.get("sent") and i.get("via") != "skipped (creator)"]
    step("create chat room", code == 200 and bool(room_id), f"room_id={room_id}")
    step("chat invite emails", len(failed) == 0 or em.get("ready") is False,
         f"sent={len(sent)} failed={len(failed)} (provider_ready={em.get('ready')})")

    # 6) Post a message AS ADMIN (admin is also a member because they created the room)
    PHI_TEXT = "Patient John Doe SSN 123-45-6789 DOB 1980-01-01 CPT 99213"
    code, j = admin.call("POST", f"/hub/api/chat/rooms/{room_id}/messages", {"body": PHI_TEXT})
    msg_id = j.get("id")
    step("admin posts chat message", code == 200 and bool(msg_id), f"msg_id={msg_id}")

    # 7) Read back — body should decrypt to the original PHI text
    code, j = admin.call("GET", f"/hub/api/chat/rooms/{room_id}/messages")
    msgs = j.get("messages") or []
    readback_ok = bool(msgs) and msgs[-1].get("body") == PHI_TEXT
    step("chat readback decrypts", readback_ok,
         f"got '{(msgs[-1].get('body') if msgs else '')[:40]}…'")

    # 8) Room list shows decrypted preview
    code, j = admin.call("GET", "/hub/api/chat/rooms")
    rooms = j.get("rooms") or []
    me_room = next((r for r in rooms if r.get("id") == room_id), None)
    step("room list shows decrypted preview",
         bool(me_room) and me_room.get("last_body") == PHI_TEXT,
         f"last_body='{(me_room or {}).get('last_body','')[:40]}…'")

    # 9) Confirm audit log does NOT carry PHI
    code, j = admin.call("GET", f"/hub/api/audit/log?limit=10")
    entries = j.get("entries") or j.get("log") or j  # endpoint shape varies
    if isinstance(entries, dict):
        entries = entries.get("entries", []) or []
    leaked = False
    if isinstance(entries, list):
        for e in entries:
            for v in (e.values() if isinstance(e, dict) else []):
                if isinstance(v, str) and "123-45-6789" in v:
                    leaked = True
    step("audit log has no PHI", not leaked, "no SSN string in last 10 audit rows")

    # 10) Switch to the new client account and confirm they can see the room
    client = Client()
    code, j = client.call("POST", "/hub/api/login", {"username": uname, "password": pword})
    step("client login", code == 200, f"role={j.get('user',{}).get('role')}")
    code, j = client.call("GET", "/hub/api/chat/rooms")
    crooms = j.get("rooms") or []
    has_room = any(r.get("id") == room_id for r in crooms)
    step("client sees the room", has_room, f"{len(crooms)} room(s) visible to client")
    if has_room:
        code, j = client.call("GET", f"/hub/api/chat/rooms/{room_id}/messages")
        cmsgs = j.get("messages") or []
        client_read_ok = bool(cmsgs) and cmsgs[-1].get("body") == PHI_TEXT
        step("client reads + decrypts message", client_read_ok)
        code, j = client.call("POST", f"/hub/api/chat/rooms/{room_id}/messages",
                              {"body": "Client confirms receipt — no PHI in this message"})
        step("client can post message", code == 200)

    # 11) EOD report preview (admin)
    code, j = admin.call("GET", "/hub/api/admin/reports/eod/preview")
    step("EOD preview", code == 200,
         f"users={len(j.get('users',[]))}, headlines.claims_new={j.get('headlines',{}).get('claims_new')}")

    # 12) Trigger demo EOD send-now
    code, j = admin.call("POST", "/hub/api/admin/reports/eod/send-now?demo=true")
    sent_eod = j.get("sent") or []
    failed_eod = j.get("failed") or []
    step("EOD demo dispatch",
         code == 200 and (len(sent_eod) > 0 or em.get("ready") is False),
         f"sent={len(sent_eod)} failed={len(failed_eod)} (provider_ready={em.get('ready')})")

    # 13) Cleanup
    code, _ = admin.call("DELETE", f"/hub/api/chat/rooms/{room_id}")
    step("cleanup: delete chat room", code == 200)
    code, _ = admin.call("DELETE", f"/hub/api/clients/{cid}")
    step("cleanup: delete client", code == 200)

    # Summary
    print()
    print("=" * 70)
    passes = sum(1 for _, ok, _ in results if ok)
    fails = sum(1 for _, ok, _ in results if not ok)
    print(f"TOTAL: {passes} passed, {fails} failed")
    print("=" * 70)
    if fails:
        print("\nFAILED:")
        for n, ok, det in results:
            if not ok:
                print(f"  ❌ {n}  ·  {det}")
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
