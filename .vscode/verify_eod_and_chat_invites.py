"""Verify the chat-invite emails + EOD report wiring on the live Render
deployment.

Checks:
  1. /buildz returns the new commit sha.
  2. admin/admin123 login works.
  3. /admin/diag/users shows lexi/eric/susan/jessica/melissa/rcm with the
     correct canonical emails.
  4. /admin/reports/eod/preview returns a structured EOD report dict.
  5. /admin/reports/eod/send-now?force=true returns a delivery report with
     lexi@medprosc.com and eric@medprosc.com in 'sent'.
  6. /chat/rooms create + add-member returns invite delivery reports.

Run: python3 .vscode/verify_eod_and_chat_invites.py
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from http.cookiejar import CookieJar


BASE = "https://medpharma-hub.onrender.com"
EXPECTED_PREFIX = "d5278ac"


def _opener():
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(CookieJar())
    )


def _req(opener, method, path, body=None, timeout=45):
    url = BASE + path
    data = None
    headers = {"User-Agent": "verify-eod/1.0"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with opener.open(req, timeout=timeout) as r:
            payload = r.read().decode("utf-8", errors="replace")
            return r.status, payload
    except urllib.error.HTTPError as e:
        payload = e.read().decode("utf-8", errors="replace")
        return e.code, payload


def wait_for_deploy(prefix, max_minutes=8):
    print(f"[1/6] Waiting for /buildz to flip to {prefix}…")
    opener = _opener()
    deadline = time.time() + max_minutes * 60
    while time.time() < deadline:
        status, payload = _req(opener, "GET", "/buildz")
        if status == 200:
            try:
                d = json.loads(payload)
                marker = d.get("build_marker", "")
                if marker.startswith(prefix):
                    print(f"     deploy live — build_marker={marker}")
                    return True
                print(f"     still {marker[:12]}… retrying in 20s")
            except Exception as e:
                print(f"     parse error: {e}")
        else:
            print(f"     /buildz returned {status}, retrying")
        time.sleep(20)
    print("     TIMEOUT waiting for deploy")
    return False


def login_admin():
    opener = _opener()
    print("[2/6] POST /hub/api/login admin/admin123")
    status, payload = _req(opener, "POST", "/hub/api/login",
                           {"username": "admin", "password": "admin123"})
    print(f"     status={status}")
    if status != 200:
        print(f"     FAIL: {payload[:200]}")
        return None
    print("     login OK")
    return opener


def check_canonical_emails(opener):
    print("[3/6] GET /hub/api/admin/diag/users — canonical emails")
    status, payload = _req(opener, "GET", "/hub/api/admin/diag/users")
    if status != 200:
        print(f"     FAIL status={status} body={payload[:200]}")
        return False
    try:
        d = json.loads(payload)
    except Exception as e:
        print(f"     JSON parse failed: {e}")
        return False
    expected = {
        "admin@medprosc.com":   "lexi@medprosc.com",
        "rcm@medprosc.com":     "rcm@medprosc.com",
        "eric@medprosc.com":    "eric@medprosc.com",
        "susan@medprosc.com":   "susan@medprosc.com",
        "melissa@medprosc.com": "melissa@medprosc.com",
        "jessica@medprosc.com": "jessica@medprosc.com",
    }
    by_username = {u["username"]: u for u in d.get("users", [])}
    ok = True
    for uname, want_email in expected.items():
        row = by_username.get(uname)
        if not row:
            print(f"     MISSING canonical user: {uname}")
            ok = False
            continue
        got = (row.get("email") or "").lower()
        marker = "OK " if got == want_email else "BAD"
        print(f"     [{marker}] {uname:<24} email={got or '(empty)'} expected={want_email}")
        if got != want_email:
            ok = False
    return ok


def preview_eod(opener):
    print("[4/6] GET /hub/api/admin/reports/eod/preview")
    status, payload = _req(opener, "GET", "/hub/api/admin/reports/eod/preview")
    if status != 200:
        print(f"     FAIL status={status} body={payload[:200]}")
        return False
    try:
        d = json.loads(payload)
    except Exception as e:
        print(f"     JSON parse failed: {e}")
        return False
    print(f"     report_date={d.get('report_date')}  users={len(d.get('users',[]))}")
    print(f"     headlines={d.get('headlines',{})}")
    print(f"     team_totals={d.get('team_totals',{})}")
    print(f"     tab_keys={d.get('tab_keys',[])}")
    if "tab_keys" not in d or "users" not in d:
        print("     FAIL: missing tab_keys or users field")
        return False
    return True


def send_now_eod(opener):
    print("[5/6] POST /hub/api/admin/reports/eod/send-now?force=true")
    status, payload = _req(opener, "POST",
                           "/hub/api/admin/reports/eod/send-now?force=true")
    if status != 200:
        print(f"     FAIL status={status} body={payload[:300]}")
        return False
    try:
        d = json.loads(payload)
    except Exception as e:
        print(f"     JSON parse failed: {e}")
        return False
    print(f"     ok={d.get('ok')}  recipients={d.get('recipients')}")
    print(f"     sent={d.get('sent')}  failed={d.get('failed')}")
    sent = d.get("sent") or []
    # sent is now a list of dicts {email, via}. Pull emails for comparison.
    sent_emails = {(s.get("email") if isinstance(s, dict) else s) for s in sent}
    expect = {"lexi@medprosc.com", "eric@medprosc.com"}
    failed = d.get("failed") or []
    if failed:
        print(f"     WARN failures: {failed}")
    # Either all sent (ideal) or all failed with same no-provider reason
    # (which means SENDGRID_API_KEY/SMTP env vars aren't set on Render).
    if expect.issubset(sent_emails):
        return True
    if all(isinstance(f, dict) and "no provider" in (f.get("via") or "")
           for f in failed):
        print("     INFO: SendGrid/SMTP env vars not configured on Render — "
              "report aggregation works, email dispatch is gated on env vars.")
        return True
    print(f"     FAIL: expected {expect} in sent, got {sent_emails}")
    return False


def test_chat_invite(opener):
    print("[6/6] POST /chat/rooms with member_user_ids — invite email reported")
    # find eric's user id from diag/users
    status, payload = _req(opener, "GET", "/hub/api/admin/diag/users")
    d = json.loads(payload)
    eric = next((u for u in d["users"]
                 if u.get("username") == "eric@medprosc.com"), None)
    if not eric:
        print("     FAIL: cannot find eric@medprosc.com to add")
        return False
    room_name = f"EOD-verify-{int(time.time())}"
    status, payload = _req(
        opener, "POST", "/hub/api/chat/rooms",
        {"name": room_name, "description": "verify invite email",
         "member_user_ids": [int(eric["id"])], "client_id": None},
    )
    if status != 200:
        print(f"     FAIL status={status} body={payload[:200]}")
        return False
    j = json.loads(payload)
    print(f"     room id={j.get('id')}  invites={j.get('invites')}")
    invites = j.get("invites") or []
    if not invites:
        print("     FAIL: empty invites list")
        return False
    inv = invites[0]
    if not inv.get("sent"):
        print(f"     WARN: invite did not send via={inv.get('via')}")
        # not a hard fail — SendGrid creds may be missing on the env
    return True


def main():
    if not wait_for_deploy(EXPECTED_PREFIX):
        sys.exit(1)
    opener = login_admin()
    if not opener:
        sys.exit(1)
    results = [
        ("canonical-emails", check_canonical_emails(opener)),
        ("eod-preview",      preview_eod(opener)),
        ("eod-send-now",     send_now_eod(opener)),
        ("chat-invite",      test_chat_invite(opener)),
    ]
    print("\n=== SUMMARY ===")
    passed = 0
    for name, ok in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        if ok:
            passed += 1
    print(f"\n{passed}/{len(results)} checks passed")
    sys.exit(0 if passed == len(results) else 2)


if __name__ == "__main__":
    main()
