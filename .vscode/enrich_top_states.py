import subprocess, urllib.request, urllib.error, time, json, http.cookiejar
WD="/workspaces/CVOPro"
BASE="https://medpharma-hub.onrender.com"

def sh(*a):
    r = subprocess.run(a, cwd=WD, capture_output=True, text=True)
    print(f"$ {' '.join(a)}\n{r.stdout}{r.stderr}")
    return r

sh("git","add","app/leads_app.py")
sh("git","commit","-m","fix: filter junk emails (placeholder, personal-mail) from enrichment","--","app/leads_app.py")
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

# Run enrichment per state, sequentially
states = ["FL","CA","TX","NY","GA","NJ","IL","NC","OH","PA","AZ","MI","CO","TN","MA"]

def trigger(st):
    url = f"{BASE}/admin/leads/api/admin/labs/enrich-batch?state={st}&tier=A&limit=30"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=30) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"err": str(e)}

def wait_done(label, max_polls=80):
    for i in range(max_polls):
        time.sleep(6)
        try:
            with urllib.request.urlopen(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=20) as r:
                d = json.loads(r.read().decode())
                if not d.get("running"):
                    print(f"  {label}: DONE -> {d.get('last_result')}")
                    return d.get("last_result")
        except Exception:
            pass
    print(f"  {label}: TIMEOUT")
    return None

for st in states:
    print(f"\n=== {st} Tier A enrich ===")
    res = trigger(st)
    print(f"  trigger: {res}")
    wait_done(st)

# final tally
print("\n=== final tally ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=20)
with op.open(f"{BASE}/admin/leads/api/national-pull/search?has_email=true&limit=200", timeout=60) as r:
    d = json.loads(r.read().decode())
    print(f"national-pull search has_email=true: matched={d.get('matched')}")
    by_state = {}
    for row in d.get('rows', []):
        s = row.get('State','?')
        by_state[s] = by_state.get(s,0)+1
    print(f"  by state: {by_state}")
    print(f"\n  sample (first 15):")
    for row in d.get('rows', [])[:15]:
        em = row.get('DM Email') or row.get('Company Email')
        print(f"    {row.get('Org Name')[:50]:50} | {row.get('State')} | {em}")
