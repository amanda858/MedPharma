"""Full end-to-end admin hub smoke test."""

import http.cookiejar
import json
import urllib.error
import urllib.request

BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def req(method, url, data=None, headers=None, timeout=30):
    payload = None
    merged_headers = {"Accept": "*/*"}
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
            return response.status, response.headers, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return -1, {}, str(exc)


results = {}

print("[1] /healthz")
status, _, body = req("GET", f"{BASE}/healthz")
print(f"   HTTP {status}  {body[:120]}")
results["healthz"] = status == 200

print("\n[2] /readyz")
status, _, body = req("GET", f"{BASE}/readyz")
print(f"   HTTP {status}  {body[:160]}")
results["readyz"] = status == 200

print("\n[3] /hub login page")
status, headers, body = req("GET", f"{BASE}/hub")
ctype = headers.get("Content-Type", "") if hasattr(headers, "get") else ""
print(f"   HTTP {status}  size={len(body)}  ctype={ctype}")
results["login_page"] = status == 200 and "text/html" in ctype

print("\n[4] POST /hub/api/login admin/admin123")
status, _, body = req("POST", f"{BASE}/hub/api/login", data={"username": "admin", "password": "admin123"})
auth_cookie = next((cookie for cookie in cj if cookie.name == "hub_session"), None)
print(f"   HTTP {status}  cookie_set={auth_cookie is not None}  body={body[:200]}")
results["login"] = status == 200 and auth_cookie is not None

print("\n[5] GET /hub/api/me")
status, _, body = req("GET", f"{BASE}/hub/api/me")
print(f"   HTTP {status}  {body[:200]}")
results["me"] = status == 200

print("\n[6] GET /hub/api/clients")
status, _, body = req("GET", f"{BASE}/hub/api/clients")
client_count = "n/a"
if status == 200:
    try:
        client_count = len(json.loads(body))
    except Exception:
        client_count = "parse-error"
print(f"   HTTP {status}  count={client_count}")
results["clients"] = status == 200

print("\n[7] Removed /admin/leads JSON surface")
status, _, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
print(f"   HTTP {status}  {body[:160]}")
results["removed_leads_surface"] = status == 410

print("\n[8] POST /hub/api/logout")
status, _, body = req("POST", f"{BASE}/hub/api/logout")
print(f"   HTTP {status}  {body[:120]}")
results["logout"] = status == 200

print("\n[9] post-logout /hub/api/me")
status, _, body = req("GET", f"{BASE}/hub/api/me")
print(f"   HTTP {status}  {body[:120]}")
results["post_logout_unauthenticated"] = status == 401

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
for key, value in results.items():
    print(f"  {key}: {value}")
failed = [key for key, value in results.items() if value is False]
if failed:
    print(f"\nFAILED: {failed}")
else:
    print("\nALL HUB CHECKS PASSED")
