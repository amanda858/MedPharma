"""Wait for Render deploy of commit 1287dda then trigger live lead import."""
import time, urllib.request, json, sys

SHA = "1287dda"
BASE = "https://medpharma-hub.onrender.com"

def http(method, url, timeout=30):
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

print("=== WAIT FOR DEPLOY ===")
ready = False
for i in range(1, 25):
    st, body = http("GET", f"{BASE}/buildz", timeout=15)
    short = (body or "")[:200].replace("\n", " ")
    print(f"  attempt {i}: HTTP {st}  {short}")
    if st == 200 and SHA in body:
        print(f"  ✓ deploy confirmed: {SHA}")
        ready = True
        break
    time.sleep(20)

if not ready:
    print("!! deploy of 1287dda not confirmed after ~8min — proceeding anyway")

print("\n=== HEALTH CHECK ===")
st, body = http("GET", f"{BASE}/healthz", timeout=15)
print(f"  /healthz: {st}  {body[:200]}")

print("\n=== TRIGGER LIVE IMPORT ===")
url = f"{BASE}/admin/leads/api/national-pull/import-bundled"
st, body = http("POST", url, timeout=300)
print(f"  POST {url}")
print(f"  HTTP {st}")
print(f"  body: {body[:2000]}")

# Try to parse and report
try:
    data = json.loads(body)
    if data.get("ok"):
        print("\n========== LIVE IMPORT OK ==========")
        print(f"  inserted        : {data.get('inserted')}")
        print(f"  skipped         : {data.get('skipped')}")
        print(f"  tier counts     : {data.get('tier_counts')}")
        print(f"  total_in_db     : {data.get('total_in_db')}")
        print(f"  by_source       : {data.get('by_source')}")
    else:
        print("!! import returned ok=false")
except Exception as e:
    print(f"  parse error: {e}")

print("\n=== SMOKE TEST: hub home ===")
st, body = http("GET", f"{BASE}/", timeout=15)
print(f"  /: HTTP {st}  bytes={len(body)}")

print("\n=== SMOKE TEST: leads admin ===")
st, body = http("GET", f"{BASE}/admin/leads/", timeout=15)
print(f"  /admin/leads/: HTTP {st}  bytes={len(body)}")
print(f"  preview: {body[:300]}")

print("\n=== SMOKE TEST: search a Tier A lab ===")
url = f"{BASE}/admin/leads/api/leads/search?state=FL&min_score=80&limit=5"
st, body = http("GET", url, timeout=30)
print(f"  HTTP {st}")
print(f"  body: {body[:1500]}")
