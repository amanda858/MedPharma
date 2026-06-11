"""Emergency login restore — uses the existing admin force-password endpoint
to reset susan, melissa, eric to known starter passwords, and re-activates
eric so he can log in.

Runs against LIVE Render hub.
"""
import http.cookiejar
import json
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"

ACCOUNTS = [
    ("susan@medprosc.com",   "susan123"),
    ("melissa@medprosc.com", "melissa123"),
    ("eric",                 "eric123"),     # re-activate + reset
]

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def post_json(url, data, timeout=30):
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def put_json(url, data, timeout=30):
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode(),
        method="PUT",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main():
    # 1) Login as rcm (admin)
    code, body = post_json(f"{BASE}/hub/api/login", {"username": "rcm", "password": "rcm123"})
    print(f"[login rcm] HTTP {code}: {body[:200]}")
    if code != 200:
        print("Cannot proceed — rcm/admin login failed")
        return

    # 2) Get current client roster so we can find eric's id
    code, body = get_json(f"{BASE}/hub/api/clients")
    if code != 200:
        print(f"[GET /clients] HTTP {code}: {body[:300]}")
        return
    roster = json.loads(body)
    by_username = {c["username"]: c for c in roster}

    # 3) Re-activate eric if he's deactivated
    eric = by_username.get("eric")
    if eric and not eric.get("is_active"):
        cid = eric["id"]
        # PUT /clients/{cid} accepts is_active in ClientUpdate
        code, body = put_json(f"{BASE}/hub/api/clients/{cid}", {"is_active": 1})
        print(f"[reactivate eric id={cid}] HTTP {code}: {body[:200]}")
    elif eric:
        print(f"[eric] already active (id={eric['id']})")
    else:
        print("[eric] not found in roster — skipping")

    # 4) Force-reset each account to its starter password
    print("\n--- Force-resetting passwords ---")
    for username, password in ACCOUNTS:
        if username == "eric" and "eric" not in by_username:
            continue
        code, body = post_json(
            f"{BASE}/hub/api/admin/users/force-password",
            {"username": username, "new_password": password},
        )
        print(f"  {username:30s} -> HTTP {code}  {body[:160]}")

    # 5) Verify each account can actually log in (fresh cookie jar each time)
    print("\n--- Verifying logins ---")
    for username, password in ACCOUNTS:
        if username == "eric" and "eric" not in by_username:
            continue
        fresh_cj = http.cookiejar.CookieJar()
        fresh_opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(fresh_cj)
        )
        req = urllib.request.Request(
            f"{BASE}/hub/api/login",
            data=json.dumps({"username": username, "password": password}).encode(),
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with fresh_opener.open(req, timeout=30) as r:
                print(f"  {username:30s} -> HTTP {r.status}  ✓ LOGIN OK")
        except urllib.error.HTTPError as e:
            print(f"  {username:30s} -> HTTP {e.code}  ✗ FAIL: {e.read().decode()[:200]}")

    # 6) Also confirm existing working ones still work
    print("\n--- Sanity check (jessica + rcm) ---")
    for u, p in [("jessica", "jessica123"), ("rcm", "rcm123")]:
        fresh_cj = http.cookiejar.CookieJar()
        fresh_opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(fresh_cj)
        )
        req = urllib.request.Request(
            f"{BASE}/hub/api/login",
            data=json.dumps({"username": u, "password": p}).encode(),
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with fresh_opener.open(req, timeout=30) as r:
                print(f"  {u:30s} -> HTTP {r.status}  ✓")
        except urllib.error.HTTPError as e:
            print(f"  {u:30s} -> HTTP {e.code}  ✗ {e.read().decode()[:150]}")


if __name__ == "__main__":
    main()
