"""
Prove out:

A) Client login link + staff access scoping:
   - Create client account "Acme Practice"
   - Grant Susan access to Acme, do NOT grant Melissa
   - Verify: Susan sees Acme in /hub/api/accounts; Melissa does NOT
   - Verify: Acme client login sees ONLY Acme's data, not other clients

B) Two-chat-room model:
   - Create "Acme <-> MedPharma" room: members = [Acme client + Susan]
   - Create "MedPharma Internal" room: members = [Susan + Melissa], NO client
   - Verify: Acme client sees ONLY the client room (NOT the internal room)
   - Verify: Susan sees BOTH rooms
   - Verify: Melissa sees ONLY the internal room (NOT Acme's room)
"""
from __future__ import annotations
import json, sys, time
import urllib.request, urllib.error

BASE = "https://medpharma-hub.onrender.com"

class Session:
    def __init__(self): self.cookie = ""
    def request(self, method, path, body=None):
        url = BASE + path
        headers = {}
        if self.cookie: headers["Cookie"] = self.cookie
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=60)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(errors="replace")
        set_cookie = resp.headers.get("Set-Cookie","")
        for ck in set_cookie.split(", "):
            if ck.startswith("hub_session="):
                self.cookie = ck.split(";",1)[0]
        return resp.status, resp.read().decode(errors="replace")
    def login(self, u, p):
        c, b = self.request("POST", "/hub/api/login", {"username":u,"password":p})
        if c != 200: raise RuntimeError(f"login {u} -> {c}: {b[:120]}")

def jload(t):
    try: return json.loads(t)
    except: return {}

def _list_from(d, keys=("accounts","clients","rooms")):
    if isinstance(d, list): return d
    if isinstance(d, dict):
        for k in keys:
            v = d.get(k)
            if isinstance(v, list): return v
    return []

def main():
    admin = Session(); admin.login("admin","admin123")
    stamp = int(time.time())

    # Find Susan + Melissa user ids
    _, b = admin.request("GET", "/hub/api/admin/users")
    users = jload(b) if isinstance(jload(b), list) else jload(b).get("users", [])
    by_name = {u["username"]: u for u in users}
    susan_id   = by_name["susan@medprosc.com"]["id"]
    melissa_id = by_name["melissa@medprosc.com"]["id"]
    print(f"  susan_id={susan_id}  melissa_id={melissa_id}")

    # ── A1: Create Acme client ────────────────────────────────────────────
    c, b = admin.request("POST", "/hub/api/clients", {
        "company": f"Acme Test {stamp}",
        "contact_name": "Acme Owner",
        "email": f"acme_{stamp}@example.com",
        "role": "client",
    })
    d = jload(b); acme_id = d["id"]; acme_login = d["login"]
    print(f"  Acme client: id={acme_id} u={acme_login['username']}")

    # Also create a SECOND unrelated client to prove cross-leak rejection
    c, b = admin.request("POST", "/hub/api/clients", {
        "company": f"Other Test {stamp}",
        "contact_name": "Other",
        "email": f"other_{stamp}@example.com",
        "role": "client",
    })
    other_id = jload(b)["id"]
    other_login = jload(b)["login"]

    # ── A2: Grant Susan access to Acme, deny Melissa ──────────────────────
    c, b = admin.request("PUT", f"/hub/api/clients/{acme_id}/access",
                         {"user_ids": [susan_id]})
    print(f"  grant Susan -> Acme: {c}")

    # ── A3: Susan logs in and lists accounts ──────────────────────────────
    susan = Session(); susan.login("susan@medprosc.com","susan123")
    _, b = susan.request("GET", "/hub/api/accounts")
    susan_accts = _list_from(jload(b))
    susan_sees_acme  = any(int(a.get("id")) == acme_id for a in susan_accts)
    susan_sees_other = any(int(a.get("id")) == other_id for a in susan_accts)
    print(f"  Susan accounts: sees_acme={susan_sees_acme} sees_other={susan_sees_other}")

    # ── A4: Melissa logs in and lists accounts ────────────────────────────
    melissa = Session(); melissa.login("melissa@medprosc.com","melissa123")
    _, b = melissa.request("GET", "/hub/api/accounts")
    mel_accts = _list_from(jload(b))
    melissa_sees_acme = any(int(a.get("id")) == acme_id for a in mel_accts)
    print(f"  Melissa accounts: sees_acme={melissa_sees_acme}")

    # ── A5: Acme client logs in, can ONLY see Acme ────────────────────────
    acme = Session(); acme.login(acme_login["username"], acme_login["password"])
    _, b = acme.request("GET", "/hub/api/me")
    me = jload(b)
    _, b = acme.request("GET", "/hub/api/accounts")
    acme_accts = _list_from(jload(b))
    acme_acct_count = len(acme_accts)
    acme_only_self = all(int(a.get("id")) == acme_id for a in acme_accts)
    print(f"  Acme client: me.role={me.get('role')} accounts={acme_acct_count} only_self={acme_only_self}")

    # Verify acme cannot read other client's data
    c, b = acme.request("GET", f"/hub/api/claims?client_id={other_id}")
    other_claims = jload(b).get("claims", []) if isinstance(jload(b), dict) else []
    leak_other = any(int(x.get("client_id") or 0) == int(other_id) for x in other_claims)
    print(f"  Acme tried to query other client claims: leak={leak_other}")

    # ── B1: Create CLIENT room (Acme <-> Susan) ───────────────────────────
    c, b = admin.request("POST", "/hub/api/chat/rooms", {
        "name": f"Acme client room {stamp}",
        "description": "External: Acme + Susan",
        "client_id": acme_id,
        "member_user_ids": [acme_id, susan_id],
    })
    client_room = jload(b)
    client_room_id = client_room.get("id") or client_room.get("room_id")
    print(f"  client room: {c} id={client_room_id}")

    # ── B2: Create INTERNAL room (Susan + Melissa, NO client) ─────────────
    c, b = admin.request("POST", "/hub/api/chat/rooms", {
        "name": f"MedPharma internal {stamp}",
        "description": "Internal team only",
        "client_id": None,
        "member_user_ids": [susan_id, melissa_id],
    })
    internal_room = jload(b)
    internal_room_id = internal_room.get("id") or internal_room.get("room_id")
    print(f"  internal room: {c} id={internal_room_id}")

    def list_rooms(sess):
        _, b = sess.request("GET", "/hub/api/chat/rooms")
        return _list_from(jload(b))

    # ── B3: Acme client sees ONLY the client room ─────────────────────────
    acme_rooms = list_rooms(acme)
    acme_room_ids = {int(r.get("id")) for r in acme_rooms}
    acme_sees_client_room   = client_room_id in acme_room_ids
    acme_sees_internal_room = internal_room_id in acme_room_ids
    print(f"  Acme rooms: {acme_room_ids}  sees_client={acme_sees_client_room} sees_internal={acme_sees_internal_room}")

    # ── B4: Susan sees both ────────────────────────────────────────────────
    susan_rooms_now = list_rooms(susan)
    susan_room_ids = {int(r.get("id")) for r in susan_rooms_now}
    susan_sees_client   = client_room_id in susan_room_ids
    susan_sees_internal = internal_room_id in susan_room_ids
    print(f"  Susan rooms: {susan_room_ids}  client={susan_sees_client} internal={susan_sees_internal}")

    # ── B5: Melissa sees only internal ────────────────────────────────────
    mel_rooms_now = list_rooms(melissa)
    mel_room_ids = {int(r.get("id")) for r in mel_rooms_now}
    mel_sees_client   = client_room_id in mel_room_ids
    mel_sees_internal = internal_room_id in mel_room_ids
    print(f"  Melissa rooms: {mel_room_ids}  client={mel_sees_client} internal={mel_sees_internal}")

    # ── Cleanup ───────────────────────────────────────────────────────────
    if client_room_id:   admin.request("DELETE", f"/hub/api/chat/rooms/{client_room_id}")
    if internal_room_id: admin.request("DELETE", f"/hub/api/chat/rooms/{internal_room_id}")
    admin.request("DELETE", f"/hub/api/clients/{acme_id}")
    admin.request("DELETE", f"/hub/api/clients/{other_id}")

    # ── Verdict ───────────────────────────────────────────────────────────
    checks = [
        ("Client account created with own login",       bool(acme_login.get("password"))),
        ("Susan granted access SEES Acme in account list", susan_sees_acme),
        ("Melissa NOT granted access does NOT see Acme",  not melissa_sees_acme),
        ("Acme client sees ONLY own account",             acme_only_self and acme_acct_count == 1),
        ("Acme cannot read other client data",            not leak_other),
        ("Client room visible to Acme",                   acme_sees_client_room),
        ("Internal room HIDDEN from Acme",                not acme_sees_internal_room),
        ("Susan sees BOTH rooms",                         susan_sees_client and susan_sees_internal),
        ("Melissa sees ONLY internal room",               mel_sees_internal and not mel_sees_client),
    ]
    print("\n" + "="*64)
    all_ok = True
    for label, ok in checks:
        if not ok: all_ok = False
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    print("="*64)
    print("ALL GREEN" if all_ok else "SOMETHING FAILED")
    return 0 if all_ok else 1

if __name__ == "__main__":
    sys.exit(main())
