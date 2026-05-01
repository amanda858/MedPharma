import urllib.request, http.cookiejar, json
BASE="https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

# login
rq = urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST")
op.open(rq, timeout=20).read()

# fetch the actual served HTML
with op.open(f"{BASE}/admin/leads/", timeout=30) as r:
    html = r.read().decode()
print("HTML size:", len(html))
# look for has_email checkbox state
import re
m = re.search(r'<input[^>]*id="nsHasEmail"[^>]*>', html)
print("nsHasEmail tag:", m.group(0) if m else "NOT FOUND")
# look for description
m = re.search(r'(Live search over[^<]+)', html)
print("Description:", m.group(1)[:200] if m else "NOT FOUND")

# Now hit the search endpoint exactly like the UI does (auth'd, unchecked, defaults)
print("\n=== UI-equivalent search (has_email=false, limit=100) ===")
url = f"{BASE}/admin/leads/api/national-pull/search?state=&specialty=&q=&has_email=false&min_heat=0&limit=100&offset=0"
with op.open(url, timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"total={d.get('total')} matched={d.get('matched')} rows={len(d.get('rows',[]))}")
    for row in d.get('rows', [])[:3]:
        print(f"  - {row.get('Org Name')} | {row.get('State')} | tier={row.get('Tier')}")

print("\n=== UI search with has_email=true ===")
url = f"{BASE}/admin/leads/api/national-pull/search?has_email=true&limit=100"
with op.open(url, timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"total={d.get('total')} matched={d.get('matched')} (will be 0 — bundled rows have no enriched email)")
