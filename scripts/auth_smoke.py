"""Authenticated smoke test of the supported hub runtime."""

import http.cookiejar
import json
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def req(method, url, data=None, headers=None, timeout=30):
    payload = None
    merged_headers = {"Accept": "application/json"}
    if headers:
        merged_headers.update(headers)
    if data is not None and not isinstance(data, bytes):
        payload = json.dumps(data).encode("utf-8")
        merged_headers["Content-Type"] = "application/json"
    else:
        payload = data
    request = urllib.request.Request(url, data=payload, method=method, headers=merged_headers)
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return -1, str(exc)


def print_check(label, ok, detail):
    print(f"[{ 'OK' if ok else 'FAIL' }] {label}: {detail}")


status, body = req("GET", f"{BASE}/readyz")
print_check("/readyz", status == 200, f"HTTP {status} {body[:160]}")

status, body = req("POST", f"{BASE}/hub/api/login", data={"username": "admin", "password": "admin123"})
print_check("admin login", status == 200, f"HTTP {status} {body[:200]}")

status, body = req("GET", f"{BASE}/hub/api/me")
print_check("/hub/api/me", status == 200, f"HTTP {status} {body[:200]}")

status, body = req("GET", f"{BASE}/hub/api/clients")
client_count = "n/a"
if status == 200:
    try:
        client_count = len(json.loads(body))
    except Exception:
        client_count = "parse-error"
print_check("/hub/api/clients", status == 200, f"HTTP {status} count={client_count}")

status, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
print_check("removed /admin/leads surface", status == 410, f"HTTP {status} {body[:120]}")

status, body = req("POST", f"{BASE}/hub/api/logout")
print_check("logout", status == 200, f"HTTP {status} {body[:120]}")

status, body = req("GET", f"{BASE}/hub/api/me")
print_check("post-logout /hub/api/me", status == 401, f"HTTP {status} {body[:120]}")
