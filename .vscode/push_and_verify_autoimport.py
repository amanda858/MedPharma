import subprocess, urllib.request, time, json
WD="/workspaces/CVOPro"
def sh(*a):
    r = subprocess.run(a, cwd=WD, capture_output=True, text=True)
    print(f"$ {' '.join(a)}\n{r.stdout}{r.stderr}")
    return r

sh("git","add","app/leads_app.py")
sh("git","commit","-m","fix: bulk-insert rule-intercept import in background thread (avoid boot timeout)","--","app/leads_app.py")
sh("git","push","origin","main")

print("\n=== capturing pushed sha ===")
sha = subprocess.run(["git","rev-parse","HEAD"], cwd=WD, capture_output=True, text=True).stdout.strip()
print("sha:", sha)

print("\n=== wait for deploy ===")
BASE="https://medpharma-hub.onrender.com"
for i in range(60):
    try:
        with urllib.request.urlopen(f"{BASE}/buildz", timeout=15) as resp:
            body = resp.read().decode()
            print(f"  attempt {i+1}: {body[:200]}")
            if sha[:8] in body:
                print("  >>> NEW BUILD LIVE")
                break
    except Exception as e:
        print(f"  attempt {i+1}: ERR {e}")
    time.sleep(10)

print("\n=== wait for background import to finish ===")
for i in range(30):
    time.sleep(5)
    try:
        # use the import-bundled endpoint? no — just check stats anonymously won't work.
        # use a public endpoint? login first.
        import http.cookiejar
        cj = http.cookiejar.CookieJar()
        op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        rq = urllib.request.Request(f"{BASE}/hub/api/login",
            data=json.dumps({"username":"admin","password":"admin123"}).encode(),
            headers={"Content-Type":"application/json"}, method="POST")
        op.open(rq, timeout=15).read()
        with op.open(f"{BASE}/admin/leads/api/leads/stats", timeout=20) as r:
            d = json.loads(r.read().decode())
            total = d.get("total_leads", 0)
            print(f"  attempt {i+1}: total_leads={total} top_states={d.get('top_states')}")
            if total > 13000:
                print("  >>> AUTO-IMPORT COMPLETE")
                break
    except Exception as e:
        print(f"  attempt {i+1}: ERR {e}")

print("\n=== final tier A FL probe ===")
import http.cookiejar
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
