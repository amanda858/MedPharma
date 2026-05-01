#!/usr/bin/env python3
import json, urllib.request, http.cookiejar
BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
# anonymous
try:
    r = op.open(f"{BASE}/admin/leads/", timeout=60)
    print("anon /admin/leads/", r.status, "len", len(r.read()))
except urllib.error.HTTPError as e:
    print("anon /admin/leads/ HTTP", e.code, e.headers.get("location"))
# login as admin via hub
data = json.dumps({"username":"admin","password":"admin123"}).encode()
r = op.open(urllib.request.Request(f"{BASE}/hub/api/login", data=data, headers={"Content-Type":"application/json"}, method="POST"), timeout=60)
print("login", r.status)
print("cookies:", [c.name for c in cj])
try:
    r = op.open(f"{BASE}/admin/leads/", timeout=60)
    body = r.read().decode("utf-8","replace")
    print("admin /admin/leads/", r.status, "len", len(body))
    print("first 200:", body[:200])
except urllib.error.HTTPError as e:
    print("admin /admin/leads/ HTTP", e.code, e.headers.get("location"))
    print(e.read().decode("utf-8","replace")[:300])
# try API
try:
    r = op.open(f"{BASE}/admin/leads/api/prospect/specialties", timeout=60)
    print("specialties", r.status, "len", len(r.read()))
except urllib.error.HTTPError as e:
    print("specialties HTTP", e.code)
    print(e.read().decode("utf-8","replace")[:300])
