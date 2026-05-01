import subprocess, urllib.request, time, json, http.cookiejar
WD="/workspaces/CVOPro"
BASE="https://medpharma-hub.onrender.com"

def sh(*a):
    r = subprocess.run(a, cwd=WD, capture_output=True, text=True)
    print(f"$ {' '.join(a)}\n{r.stdout}{r.stderr}")
    return r

sh("git","add","app/templates/index.html")
sh("git","commit","-m","fix: bust cached HTML id; auto-run search on load; clearer no-email hint","--","app/templates/index.html")
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
            print(f"  {i+1}: {body[:120]}")
    except Exception as e:
        print(f"  {i+1}: ERR {e}")
    time.sleep(8)

print("\n=== verify served HTML ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=20)
with op.open(f"{BASE}/admin/leads/", timeout=30) as r:
    html = r.read().decode()
import re
m = re.search(r'<input[^>]*id="nsHasEmail[^"]*"[^>]*>', html)
print("checkbox:", m.group(0) if m else "NOT FOUND")
print("auto-run:", "runNationalSearch, 2200" in html)

print("\n=== UI default search ===")
url = f"{BASE}/admin/leads/api/national-pull/search?has_email=false&limit=10"
with op.open(url, timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"total={d.get('total')} matched={d.get('matched')}")
    for row in d.get('rows', [])[:3]:
        print(f"  {row.get('Org Name')} | {row.get('State')} tier={row.get('Tier')}")
