import subprocess, urllib.request, urllib.error, time, json, http.cookiejar
WD="/workspaces/CVOPro"
BASE="https://medpharma-hub.onrender.com"

def sh(*a):
    r = subprocess.run(a, cwd=WD, capture_output=True, text=True)
    print(f"$ {' '.join(a)}\n{r.stdout}{r.stderr}")
    return r

sh("git","add","app/hub_app.py")
sh("git","commit","-m","fix: move rule-intercept auto-restore into hub_app startup (sub-app hooks dont fire on mount)","--","app/hub_app.py")
sh("git","push","origin","main")
sha = subprocess.run(["git","rev-parse","HEAD"], cwd=WD, capture_output=True, text=True).stdout.strip()
print("\nsha:", sha)

print("\n=== wait for new build ===")
for i in range(80):
    try:
        with urllib.request.urlopen(f"{BASE}/buildz", timeout=15) as resp:
            body = resp.read().decode()
            print(f"  {i+1}: {body[:150]}")
            if sha[:8] in body:
                print(f"  >>> NEW BUILD {sha[:8]} LIVE")
                break
    except Exception as e:
        print(f"  {i+1}: ERR {e}")
    time.sleep(8)

print("\n=== wait for auto-restore ===")
for i in range(40):
    time.sleep(5)
    try:
        cj = http.cookiejar.CookieJar()
        op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        rq = urllib.request.Request(f"{BASE}/hub/api/login",
            data=json.dumps({"username":"admin","password":"admin123"}).encode(),
            headers={"Content-Type":"application/json"}, method="POST")
        op.open(rq, timeout=15).read()
        with op.open(f"{BASE}/admin/leads/api/leads/stats", timeout=20) as r:
            d = json.loads(r.read().decode())
            total = d.get("total_leads", 0)
            print(f"  {i+1}: total_leads={total}")
            if total > 13000:
                print("  >>> AUTO-RESTORE COMPLETE")
                break
    except Exception as e:
        print(f"  {i+1}: ERR {e}")

print("\n=== FL tier A probe ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
rq = urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST")
op.open(rq, timeout=15).read()
with op.open(f"{BASE}/admin/leads/api/leads?state=FL&min_score=80", timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"  FL tier A count: {d.get('count')}")
    for l in (d.get('leads') or [])[:5]:
        print(f"    - {l.get('organization_name')} | {l.get('city')}, {l.get('state')} | score={l.get('lead_score')}")
