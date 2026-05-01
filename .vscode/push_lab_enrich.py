import subprocess, urllib.request, urllib.error, time, json, http.cookiejar
WD="/workspaces/CVOPro"
BASE="https://medpharma-hub.onrender.com"

def sh(*a):
    r = subprocess.run(a, cwd=WD, capture_output=True, text=True)
    print(f"$ {' '.join(a)}\n{r.stdout}{r.stderr}")
    return r

sh("git","add","app/leads_app.py")
sh("git","commit","-m","feat: lab batch email enrichment + JOIN emails into search results","--","app/leads_app.py")
sh("git","push","origin","main")
sha = subprocess.run(["git","rev-parse","HEAD"], cwd=WD, capture_output=True, text=True).stdout.strip()
print("\nsha:", sha)

print("\n=== wait for deploy ===")
for i in range(80):
    try:
        with urllib.request.urlopen(f"{BASE}/buildz", timeout=15) as resp:
            body = resp.read().decode()
            if sha[:8] in body:
                print(f"  {i+1}: NEW BUILD LIVE")
                break
            print(f"  {i+1}: {body[:100]}")
    except Exception as e:
        print(f"  {i+1}: ERR {e}")
    time.sleep(8)

print("\n=== trigger lab enrichment FL Tier A limit=25 ===")
url = f"{BASE}/admin/leads/api/admin/labs/enrich-batch?state=FL&tier=A&limit=25"
with urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=30) as r:
    print(json.loads(r.read().decode()))

print("\n=== poll status for up to 5 min ===")
for i in range(60):
    time.sleep(5)
    try:
        with urllib.request.urlopen(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=15) as r:
            d = json.loads(r.read().decode())
            print(f"  {i+1}: running={d['running']} last={d.get('last_result')}")
            if not d["running"] and d.get("last_result"):
                break
    except Exception as e:
        print(f"  {i+1}: ERR {e}")

print("\n=== verify search now shows enriched emails ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=15)

with op.open(f"{BASE}/admin/leads/api/national-pull/search?state=FL&has_email=true&limit=20", timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"\nFL has_email=true: matched={d.get('matched')} returned={len(d.get('rows',[]))}")
    for row in d.get('rows', [])[:10]:
        print(f"  {row.get('Org Name')} | DM={row.get('DM Email')} | CO={row.get('Company Email')} | dom={row.get('Org Domain')}")
