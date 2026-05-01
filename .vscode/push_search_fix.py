import subprocess, urllib.request, urllib.error, time, json, http.cookiejar
WD="/workspaces/CVOPro"
BASE="https://medpharma-hub.onrender.com"

def sh(*a):
    r = subprocess.run(a, cwd=WD, capture_output=True, text=True)
    print(f"$ {' '.join(a)}\n{r.stdout}{r.stderr}")
    return r

sh("git","add","app/leads_app.py","app/templates/index.html")
sh("git","commit","-m","fix: national-pull search falls back to bundled lab CSV; default has_email=off","--","app/leads_app.py","app/templates/index.html")
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

print("\n=== national-pull/specialties (no auth needed) ===")
try:
    with urllib.request.urlopen(f"{BASE}/admin/leads/api/national-pull/specialties", timeout=30) as r:
        d = json.loads(r.read().decode())
        sp = d.get("specialties", [])
        print(f"  count: {len(sp)}")
        for s in sp[:10]:
            print(f"    - {s['name']}: {s['count']}")
except Exception as e:
    print(f"  ERR {e}")

print("\n=== national-pull/search default ===")
try:
    with urllib.request.urlopen(f"{BASE}/admin/leads/api/national-pull/search?has_email=false&limit=5", timeout=30) as r:
        d = json.loads(r.read().decode())
        print(f"  total: {d.get('total')}  matched: {d.get('matched')}")
        for row in d.get('rows', [])[:5]:
            print(f"    - {row.get('Org Name')} | {row.get('City')}, {row.get('State')} | {row.get('Taxonomy / Type')} | tier={row.get('Tier')} heat={row.get('Heat Score')}")
except Exception as e:
    print(f"  ERR {e}")

print("\n=== national-pull/search FL labs ===")
try:
    with urllib.request.urlopen(f"{BASE}/admin/leads/api/national-pull/search?state=FL&specialty=laboratory&has_email=false&limit=5", timeout=30) as r:
        d = json.loads(r.read().decode())
        print(f"  total: {d.get('total')}  matched: {d.get('matched')}")
        for row in d.get('rows', [])[:5]:
            print(f"    - {row.get('Org Name')} | {row.get('City')}, {row.get('State')} | {row.get('Taxonomy / Type')}")
except Exception as e:
    print(f"  ERR {e}")
