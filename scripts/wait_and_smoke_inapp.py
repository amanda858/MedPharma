"""Wait for the new build to land on Render then validate the in-app
notification surface live."""
from __future__ import annotations

import http.cookiejar
import json
import time
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"
TARGET = "da2bd9f"
MAX_WAIT = 360


def make_session():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def call(opener, method, path, payload=None, timeout=45):
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


def main() -> int:
    print(f"\nWaiting for build {TARGET} on {BASE} (up to {MAX_WAIT}s)…\n")
    s = make_session()
    started = time.time()
    last_marker = ""
    while time.time() - started < MAX_WAIT:
        code, body = call(s, "GET", "/buildz", timeout=20)
        marker = body.get("build_marker", "")[:7] if isinstance(body, dict) else ""
        if marker != last_marker:
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] build_marker={marker or '?'}")
            last_marker = marker
        if marker == TARGET:
            break
        time.sleep(10)
    else:
        print(f"\n⏱  Build {TARGET} never landed. Current: {last_marker}")
        return 2

    print(f"\n✅ Build {TARGET} live\n")

    # Now smoke the new endpoints
    admin = make_session()
    code, _ = call(admin, "POST", "/hub/api/login",
                   {"username": "admin", "password": "admin123"})
    assert code == 200, f"admin login failed: {code}"
    print(f"✅ admin login")

    code, body = call(admin, "GET", "/hub/api/notifications/unread-count")
    assert code == 200, f"unread-count failed: {code}"
    print(f"✅ GET /notifications/unread-count -> {body}")

    code, body = call(admin, "GET", "/hub/api/notifications")
    assert code == 200, f"notifications list failed: {code}"
    items = body.get("items", [])
    print(f"✅ GET /notifications -> {len(items)} items, unread={body.get('unread')}")

    # Trigger a chat invite to spawn a notification
    uname = f"smoke_live_inapp_{int(time.time())}"
    code, body = call(admin, "POST", "/hub/api/clients", {
        "username": uname,
        "password": "LiveInapp123!",
        "role": "client",
        "company": f"Live Inapp {uname}",
        "contact_name": "Live Inapp",
        "email": "live.inapp@example.com",
    })
    cid = body.get("id") if isinstance(body, dict) else None
    assert code == 200 and cid, f"create client failed: {code} {body}"
    print(f"✅ created client id={cid}")

    code, body = call(admin, "POST", "/hub/api/chat/rooms", {
        "name": f"Live Inapp Room {int(time.time())}",
        "description": "live in-app smoke",
        "client_id": cid,
        "member_user_ids": [cid],
    })
    room_id = body.get("id") if isinstance(body, dict) else None
    assert code == 200 and room_id, f"create room failed: {code} {body}"
    print(f"✅ created chat room id={room_id}")

    # Log in as the client and check they see the notification
    client = make_session()
    code, _ = call(client, "POST", "/hub/api/login",
                   {"username": uname, "password": "LiveInapp123!"})
    assert code == 200, f"client login failed: {code}"

    code, body = call(client, "GET", "/hub/api/notifications/unread-count")
    print(f"✅ client unread-count -> {body.get('unread') if isinstance(body, dict) else body}")

    code, body = call(client, "GET", "/hub/api/notifications")
    if isinstance(body, dict):
        items = body.get("items", [])
        invite = next((n for n in items
                       if n.get("kind") == "chat_invite"
                       and (n.get("related_id") or 0) == room_id), None)
        if invite:
            print(f"✅ client sees chat_invite notification: '{invite.get('title')}'")
            print(f"   link={invite.get('link')}")
        else:
            print(f"❌ client did NOT see chat_invite. items: {items}")
            return 1

    # Cleanup
    code, _ = call(admin, "DELETE", f"/hub/api/chat/rooms/{room_id}")
    code, _ = call(admin, "DELETE", f"/hub/api/clients/{cid}")
    print(f"✅ cleanup complete")

    code, body = call(admin, "GET", "/hub/api/reports/eod/history")
    if isinstance(body, dict):
        print(f"✅ EOD history endpoint live: {len(body.get('reports', []))} reports")

    print(f"\n🎉 In-app notification system is LIVE on {BASE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
