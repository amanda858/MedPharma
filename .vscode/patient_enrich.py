import urllib.request, json, time
BASE="https://medpharma-hub.onrender.com"

# Long-timeout polls so we can ride out the worker contention
print("=== patient poll ===")
for i in range(50):
    try:
        with urllib.request.urlopen(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=30) as r:
            d = json.loads(r.read().decode())
            print(f"  {i+1}: running={d['running']} last={d.get('last_result')}")
            if not d["running"] and d.get("last_result"):
                print("  >>> COMPLETE")
                break
    except Exception as e:
        print(f"  {i+1}: ERR {type(e).__name__}")
    time.sleep(8)

# Trigger TX enrich now that FL is done
print("\n=== trigger CA tier A 30 ===")
try:
    with urllib.request.urlopen(urllib.request.Request(
        f"{BASE}/admin/leads/api/admin/labs/enrich-batch?state=CA&tier=A&limit=30",
        method="POST"), timeout=30) as r:
        print(json.loads(r.read().decode()))
except Exception as e:
    print("ERR", e)

print("\n=== poll CA ===")
for i in range(50):
    try:
        with urllib.request.urlopen(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=30) as r:
            d = json.loads(r.read().decode())
            print(f"  {i+1}: running={d['running']} last={d.get('last_result')}")
            if not d["running"] and d.get("last_result"):
                break
    except Exception as e:
        print(f"  {i+1}: ERR {type(e).__name__}")
    time.sleep(8)

# tally
import http.cookiejar
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=20)

print("\n=== TALLY: leads with email ===")
with op.open(f"{BASE}/admin/leads/api/national-pull/search?has_email=true&limit=200", timeout=60) as r:
    d = json.loads(r.read().decode())
    print(f"matched={d.get('matched')}")
    for row in d.get('rows', [])[:30]:
        em = row.get('DM Email') or row.get('Company Email')
        print(f"  {row.get('State')} | {row.get('Org Name')[:50]:50} | {em}")
