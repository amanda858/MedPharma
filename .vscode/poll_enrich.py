import urllib.request, json, time, http.cookiejar
BASE="https://medpharma-hub.onrender.com"

print("=== poll lab enrich status ===")
for i in range(60):
    try:
        with urllib.request.urlopen(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=15) as r:
            d = json.loads(r.read().decode())
            print(f"  {i+1}: running={d['running']} last={d.get('last_result')}")
            if not d["running"] and d.get("last_result"):
                break
    except Exception as e:
        print(f"  {i+1}: ERR {e}")
    time.sleep(5)

print("\n=== verify enriched emails in search ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=15)

with op.open(f"{BASE}/admin/leads/api/national-pull/search?state=FL&has_email=true&limit=20", timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"FL has_email=true: matched={d.get('matched')}")
    for row in d.get('rows', [])[:15]:
        em = row.get('DM Email') or row.get('Company Email')
        print(f"  {row.get('Org Name')[:50]:50} | {em or '(no email)'}")

with op.open(f"{BASE}/admin/leads/api/national-pull/search?state=FL&has_email=false&limit=10", timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"\nFL all (has_email=false): matched={d.get('matched')}")
    enriched_in_page = sum(1 for r in d.get('rows',[]) if r.get('DM Email') or r.get('Company Email'))
    print(f"  rows with email in current page: {enriched_in_page}")
