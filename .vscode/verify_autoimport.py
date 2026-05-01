import urllib.request, urllib.error, time, json, http.cookiejar
BASE="https://medpharma-hub.onrender.com"
TARGET="38f88869"

print("=== wait for new build ===")
for i in range(80):
    try:
        with urllib.request.urlopen(f"{BASE}/buildz", timeout=15) as resp:
            body = resp.read().decode()
            print(f"  {i+1}: {body[:150]}")
            if TARGET in body:
                print(f"  >>> NEW BUILD {TARGET} LIVE")
                break
    except Exception as e:
        print(f"  {i+1}: ERR {e}")
    time.sleep(8)

print("\n=== wait for background import ===")
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
                print("  >>> AUTO-IMPORT COMPLETE")
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
