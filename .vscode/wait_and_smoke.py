"""Wait for new deploy SHA, then run auth smoke test."""
import time, urllib.request, json, urllib.parse, http.cookiejar, urllib.error, subprocess, os

BASE = "https://medpharma-hub.onrender.com"

# Get current local SHA short
sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd="/workspaces/CVOPro").decode().strip()
print(f"=== Waiting for deploy of SHA {sha} ===")

def http(method, url, timeout=30):
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

ready = False
for i in range(1, 30):
    st, body = http("GET", f"{BASE}/buildz", timeout=15)
    short = (body or "")[:160].replace("\n", " ")
    print(f"  attempt {i}: HTTP {st}  {short}")
    if st == 200 and sha in body:
        print(f"  ✓ deploy confirmed: {sha}")
        ready = True
        break
    time.sleep(20)

if not ready:
    print("!! deploy not confirmed; proceeding anyway")

# Auth + smoke
print("\n=== LOGIN ===")
cj = http.cookiejar.CookieJar() if False else None
import http.cookiejar as cj_mod
cj = cj_mod.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def req(method, url, data=None, timeout=60):
    h = {"Accept":"application/json"}
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        h["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=body, method=method, headers=h)
    try:
        with opener.open(r, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

st, body = req("POST", f"{BASE}/hub/api/login", data={"username":"admin","password":"admin123"})
print(f"  login: HTTP {st}  body={body[:200]}")

print("\n=== STATS ===")
st, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
print(f"  HTTP {st}  body={body[:500]}")

print("\n=== Tier A any state, min_score=80 ===")
st, body = req("GET", f"{BASE}/admin/leads/api/leads?min_score=80")
try:
    data = json.loads(body)
    leads = data.get("leads", []) if isinstance(data, dict) else data
    print(f"  HTTP {st}  count={len(leads)}")
    for L in leads[:8]:
        tags = L.get("tags","")
        print(f"    - [{L.get('lead_score','?')}] {L.get('organization_name','?')} | {L.get('city','')}, {L.get('state','')} | {tags}")
except Exception as e:
    print(f"  HTTP {st}  parse error: {e}  body={body[:400]}")

print("\n=== Tier A FL only ===")
st, body = req("GET", f"{BASE}/admin/leads/api/leads?state=FL&min_score=80")
try:
    data = json.loads(body)
    leads = data.get("leads", []) if isinstance(data, dict) else data
    print(f"  HTTP {st}  FL count={len(leads)}")
    for L in leads[:5]:
        print(f"    - {L.get('organization_name','?')} | {L.get('city','')} | score={L.get('lead_score','?')}")
except Exception as e:
    print(f"  parse: {e}")
