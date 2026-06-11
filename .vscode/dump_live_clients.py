"""Login as rcm/admin and dump the live client roster so we can see what's
actually in the production DB after deploy.
"""
import http.cookiejar
import json
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def post_json(url, data):
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with opener.open(req, timeout=30) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def get_json(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with opener.open(req, timeout=30) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main():
    # 1) Login as rcm (admin)
    code, body = post_json(f"{BASE}/hub/api/login", {"username": "rcm", "password": "rcm123"})
    print(f"[login rcm] HTTP {code}: {body[:200]}")
    if code != 200:
        return

    # 2) List clients
    code, body = get_json(f"{BASE}/hub/api/clients")
    print(f"\n[GET /clients] HTTP {code}")
    try:
        data = json.loads(body)
        # Print just username/role/is_active for everyone
        for c in data:
            print(
                f"  id={c.get('id'):>3}  active={c.get('is_active')}  "
                f"role={c.get('role'):>10}  username={c.get('username')}"
            )
    except Exception:
        print(body[:1000])


if __name__ == "__main__":
    main()
