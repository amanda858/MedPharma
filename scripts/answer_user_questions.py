"""
Verify the 5 capabilities the user is asking about, end-to-end on the live deploy:

  1. Add/remove users (people)
  2. Add/remove clients
  3. Hide EDI / Enrollment / Credentialing per-client
  4. Generate a per-client login link
  5. Per-account data scoping (records added to one client are not visible to others)
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
import urllib.request
import urllib.error

BASE = "https://medpharma-hub.onrender.com"
ADMIN = ("admin", "admin123")

class Session:
    def __init__(self) -> None:
        self.cookie = ""

    def request(self, method: str, path: str, body=None, files=None):
        url = BASE + path
        headers = {}
        if self.cookie:
            headers["Cookie"] = self.cookie
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=60)
        except urllib.error.HTTPError as e:
            txt = e.read().decode(errors="replace")
            return e.code, txt
        set_cookie = resp.headers.get("Set-Cookie", "")
        if set_cookie:
            for ck in set_cookie.split(", "):
                if ck.startswith("hub_session="):
                    self.cookie = ck.split(";", 1)[0]
        body_text = resp.read().decode(errors="replace")
        return resp.status, body_text


def jload(t: str):
    try:
        return json.loads(t)
    except Exception:
        return {"_raw": t[:300]}


def main() -> int:
    s = Session()
    stamp = int(time.time())
    new_staff = f"qtest_{stamp}@medprosc.com"
    new_client_company = f"QTest Practice {stamp}"
    results: list[tuple[str, bool, str]] = []

    # ── login as admin ──────────────────────────────────────
    code, body = s.request("POST", "/hub/api/login", {"username": ADMIN[0], "password": ADMIN[1]})
    if code != 200:
        print(f"FATAL: admin login {code}: {body[:200]}")
        return 1
    print(f"  admin login: {code}")

    # ── Q1a: Add a staff person via invite ──────────────────
    code, body = s.request("POST", "/hub/api/admin/users/invite", {
        "company": "MedPharma SC",
        "contact_name": "Q Test Staffer",
        "email": new_staff,
        "role": "staff",
        "username": f"qstaff_{stamp}",
        "initial_password": "TestPassw0rd!"
    })
    d = jload(body)
    staff_id = d.get("user_id")
    staff_username = d.get("username")
    setup_link_staff = d.get("setup_link", "")
    ok = code == 200 and staff_id and setup_link_staff
    results.append(("Q1: Add staff user (people)", bool(ok),
                    f"id={staff_id} username={staff_username} setup_link_returned={'yes' if setup_link_staff else 'no'}"))
    print(f"  add staff: {code} -> id={staff_id} setup_link={'returned' if setup_link_staff else 'MISSING'}")

    # ── Q2a: Add a client account ───────────────────────────
    code, body = s.request("POST", "/hub/api/clients", {
        "company": new_client_company,
        "contact_name": "Client Owner",
        "email": f"qclient_{stamp}@example.com",
        "role": "client",
    })
    d = jload(body)
    client_id = d.get("id")
    client_login = d.get("login") or {}
    ok = code == 200 and client_id and client_login.get("username") and client_login.get("password")
    results.append(("Q2: Add client account", bool(ok),
                    f"id={client_id} username={client_login.get('username')} pw_returned={'yes' if client_login.get('password') else 'no'}"))
    print(f"  add client: {code} -> id={client_id} u={client_login.get('username')}")

    # ── Q3: Hide EDI/Enrollment/Credentialing on that client ─
    code, body = s.request("PUT", f"/hub/api/profile/{client_id}", {
        "enabled_modules": ["dashboard", "profile", "claims", "documents", "chat"]
    })
    ok_save = code == 200
    # Read back
    code2, body2 = s.request("GET", f"/hub/api/profile/{client_id}")
    d = jload(body2)
    saved = d.get("enabled_modules") if isinstance(d, dict) else None
    hidden_correctly = (saved is not None
                       and "edi" not in (saved or [])
                       and "enrollment" not in (saved or [])
                       and "credentialing" not in (saved or []))
    results.append(("Q3: Hide EDI/Enroll/Cred per client", bool(ok_save and hidden_correctly),
                    f"saved={saved}"))
    print(f"  module hide: save={code} read={code2} -> {saved}")

    # ── Q4: Per-client login link works (the client logs in via setup_token) ──
    # The invite for a NEW client account doesn't auto-issue a setup_token; that's only via /admin/users/invite.
    # Demonstrate by inviting a client-role user (no initial password) so we get a real setup_link.
    code, body = s.request("POST", "/hub/api/admin/users/invite", {
        "company": new_client_company,
        "contact_name": "Q Test Client",
        "email": f"qclientlink_{stamp}@example.com",
        "role": "client",
        "username": f"qclientlink_{stamp}",
    })
    d = jload(body)
    setup_link_client = d.get("setup_link", "")
    cli_id = d.get("user_id")
    # Verify the token actually resolves to a "set password" page
    if setup_link_client:
        token = setup_link_client.split("setup_token=", 1)[-1]
        tcode, tbody = s.request("GET", f"/hub/api/auth/setup-password/{token}")
        tinfo = jload(tbody)
        link_resolves = tcode == 200 and bool(tinfo.get("username"))
    else:
        link_resolves = False
    ok = code == 200 and setup_link_client and link_resolves
    results.append(("Q4: Per-client login link generated and resolves", bool(ok),
                    f"link={'yes' if setup_link_client else 'no'} token_resolves={link_resolves}"))
    print(f"  client invite link: {code} resolves={link_resolves}")

    # ── Q5: Per-account scoping (claim added to client A is invisible to client B) ──
    # Add a claim scoped to our test client_id
    code, body = s.request("POST", "/hub/api/claims", {
        "client_id": client_id,
        "ClaimKey": f"QTEST-{stamp}",
        "PatientName": "Scope Test Patient",
        "DOS": "2026-06-10",
        "ChargeAmount": 250.0,
        "ClaimStatus": "Intake",
    })
    claim_created = code == 200
    claim_d = jload(body)
    claim_id = claim_d.get("id") or claim_d.get("claim_id")

    # As admin, fetch claims filtered by client_id — should include our row
    code, body = s.request("GET", f"/hub/api/claims?client_id={client_id}")
    d = jload(body)
    claims_for_us = d.get("claims") or d if isinstance(d, list) else (d.get("claims") or [])
    if not isinstance(claims_for_us, list):
        claims_for_us = []
    found_in_our_scope = any(int(c.get("client_id") or 0) == int(client_id) for c in claims_for_us)

    # Fetch claims for an OTHER existing client — should not include this one
    code, body = s.request("GET", "/hub/api/clients")
    all_clients = jload(body)
    if isinstance(all_clients, dict):
        all_clients = all_clients.get("clients") or []
    other_cid = next((c["id"] for c in all_clients if int(c["id"]) != int(client_id) and (c.get("role") or "").lower() != "admin"), None)
    leak = False
    if other_cid:
        code, body = s.request("GET", f"/hub/api/claims?client_id={other_cid}")
        d = jload(body)
        other_claims = d.get("claims") if isinstance(d, dict) else d
        if not isinstance(other_claims, list):
            other_claims = []
        leak = any(int(c.get("id") or 0) == int(claim_id or 0) for c in other_claims if claim_id)
    ok = claim_created and found_in_our_scope and not leak
    results.append(("Q5: Per-account data scoping (records pinned to one client)",
                    bool(ok),
                    f"claim_created={claim_created} found_in_own_scope={found_in_our_scope} leaks_to_other={leak}"))
    print(f"  claim scoping: create={claim_created} found={found_in_our_scope} leak={leak}")

    # ── Q1b: Remove the staff user we created ────────────────
    code, body = s.request("DELETE", f"/hub/api/clients/{staff_id}")
    ok = code == 200
    results.append(("Q1: Remove staff user", bool(ok), f"DELETE /clients/{staff_id} -> {code}"))
    print(f"  remove staff: {code}")

    # Remove the invited client-role user
    if cli_id:
        s.request("DELETE", f"/hub/api/clients/{cli_id}")

    # ── Q2b: Remove the client we created ────────────────────
    code, body = s.request("DELETE", f"/hub/api/clients/{client_id}")
    ok = code == 200
    results.append(("Q2: Remove client account", bool(ok), f"DELETE /clients/{client_id} -> {code}"))
    print(f"  remove client: {code}")

    # ── Summary ──────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("RESULTS")
    print("=" * 72)
    all_ok = True
    for label, ok, detail in results:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            all_ok = False
        print(f"  [{mark}] {label}")
        print(f"         {detail}")
    print("=" * 72)
    print("ALL GREEN" if all_ok else "SOMETHING FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
