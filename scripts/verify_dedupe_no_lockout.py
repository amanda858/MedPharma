#!/usr/bin/env python3
"""Wait for 06a632f then verify:
  1. admin/admin123 still works (the regression I caused)
  2. admin@medprosc.com/admin123 still works
  3. Chat picker shows exactly 6 users, one per real person
  4. /admin/users (Manage Clients team list) is also deduped
"""
import time, urllib.request, json, sys

BASE = "https://medpharma-hub.onrender.com"
EXPECTED = "908eea3"


def get(url, cookie=None):
    req = urllib.request.Request(url)
    if cookie: req.add_header("Cookie", cookie)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def post(url, body, cookie=None):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if cookie: req.add_header("Cookie", cookie)
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return r.getcode(), r.headers.get("Set-Cookie", "").split(";")[0], json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, None, {}


def wait():
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            d = get(f"{BASE}/buildz")
            m = (d.get("build_marker") or "")[:7]
            print(f"  current={m} want={EXPECTED}")
            if m == EXPECTED: return True
        except Exception as e:
            print(f"  err: {e}")
        time.sleep(15)
    return False


def main():
    print(f"[wait] polling for {EXPECTED}…")
    if not wait():
        sys.exit(1)
    print("[wait] ✅ live\n")

    results = []

    # 1) admin/admin123 (the broken-then-restored legacy login)
    print("== login as 'admin' / 'admin123' ==")
    code, _, d = post(f"{BASE}/hub/api/login", {"username": "admin", "password": "admin123"})
    ok = code == 200 and d.get("ok")
    print(f"  HTTP {code}  ok={ok}")
    results.append(("admin/admin123 login", ok))

    # 2) admin@medprosc.com
    print("\n== login as 'admin@medprosc.com' / 'admin123' ==")
    code, cookie, d = post(f"{BASE}/hub/api/login", {"username": "admin@medprosc.com", "password": "admin123"})
    ok = code == 200 and d.get("ok")
    print(f"  HTTP {code}  ok={ok}")
    results.append(("admin@medprosc.com login", ok))

    # 3) /chat/users
    print("\n== /hub/api/chat/users (the New Room picker) ==")
    cu = get(f"{BASE}/hub/api/chat/users", cookie=cookie)
    arr = cu if isinstance(cu, list) else cu.get("users", [])
    for u in arr:
        print(f"  {u['username']:<35} {u['contact_name']:<12} {u['role']}")
    seen_short = {u['username'] for u in arr} & {"admin","rcm","jessica","susan","melissa","eric"}
    canon = {f"{n}@medprosc.com" for n in ("admin","rcm","jessica","susan","melissa","eric")}
    seen_canon = {u['username'] for u in arr} & canon
    no_dups = not seen_short
    all_canon = len(seen_canon) == 6
    print(f"\n  total={len(arr)}  legacy_shorts_hidden={no_dups}  all_canonical_present={all_canon}")
    results.append(("chat picker hides legacy shorts", no_dups))
    results.append(("chat picker has all 6 canonical", all_canon))

    # 4) /admin/users
    print("\n== /hub/api/admin/users (Manage Clients team list) ==")
    au = get(f"{BASE}/hub/api/admin/users", cookie=cookie)
    arr2 = au if isinstance(au, list) else au.get("users", [])
    for u in arr2:
        print(f"  {u['username']:<35} {u['contact_name']:<12} {u['role']}")
    no_dups2 = not ({u['username'] for u in arr2} & {"admin","rcm","jessica","susan","melissa","eric"})
    results.append(("/admin/users hides legacy shorts", no_dups2))

    passed = sum(1 for _, v in results if v)
    print(f"\n=== {passed}/{len(results)} PASS ===")
    for label, v in results:
        print(f"  {'✅' if v else '❌'} {label}")
    if passed != len(results):
        sys.exit(2)


if __name__ == "__main__":
    main()
