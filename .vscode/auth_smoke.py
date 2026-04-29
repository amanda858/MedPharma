"""Authenticated smoke test of live hub after import."""
import urllib.request, urllib.parse, json, http.cookiejar

BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def req(method, url, data=None, headers=None, timeout=60):
    h = {"Accept": "application/json"}
    if headers: h.update(headers)
    if data is not None and not isinstance(data, bytes):
        data = json.dumps(data).encode("utf-8")
        h["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with opener.open(r, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

print("=== LOGIN ===")
st, body = req("POST", f"{BASE}/hub/api/login", data={"username":"admin","password":"admin123"})
print(f"  /hub/api/login HTTP {st}")
print(f"  body: {body[:300]}")

print("\n=== TIER A search (FL, score>=80) ===")
st, body = req("GET", f"{BASE}/admin/leads/api/leads?state=FL&min_score=80")
print(f"  HTTP {st}")
print(f"  preview: {body[:1500]}")
try:
    data = json.loads(body)
    if isinstance(data, dict) and "leads" in data:
        leads = data["leads"]
    elif isinstance(data, list):
        leads = data
    else:
        leads = []
    print(f"  count returned: {len(leads)}")
    for l in leads[:5]:
        print(f"    - {l.get('organization_name')} | {l.get('city')}, {l.get('state')} | score={l.get('lead_score')} | tags={l.get('tags')}")
except Exception as e:
    print(f"  parse: {e}")

print("\n=== TIER A search (any state) ===")
st, body = req("GET", f"{BASE}/admin/leads/api/leads?min_score=80")
print(f"  HTTP {st}")
print(f"  preview: {body[:1500]}")

print("\n=== Total saved leads (no filter) ===")
st, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
print(f"  HTTP {st}")
print(f"  preview: {body[:600]}")
try:
    data = json.loads(body)
    if isinstance(data, dict):
        for k in ("total","count","total_count"):
            if k in data: print(f"  {k} = {data[k]}")
except Exception:
    pass
