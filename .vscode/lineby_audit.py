"""Line-by-line static + live audit of the lab enrichment + search code path."""
import subprocess, urllib.request, urllib.error, json, http.cookiejar, time, re, ast, sys

WD="/workspaces/CVOPro"
BASE="https://medpharma-hub.onrender.com"

PASS, FAIL = [], []
def ok(label, cond, det=""):
    bucket = PASS if cond else FAIL
    bucket.append((label, det))
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}  {det}")

# ─── 1. Static syntax & import sanity ───────────────────────────────
print("\n=== 1. STATIC PARSE ===")
src = open(f"{WD}/app/leads_app.py").read()
try:
    tree = ast.parse(src)
    ok("leads_app.py parses", True, f"{len(src)} bytes")
except SyntaxError as e:
    ok("leads_app.py parses", False, str(e)); sys.exit(1)

# Ensure key symbols defined
syms_needed = ["_attach_emails_to_rows","_load_latest_national_rows","_bulk_enrich_labs",
               "search_national_pull","_lab_enrich_state","_load_bundled_lab_rows"]
for s in syms_needed:
    ok(f"symbol {s} defined", f"def {s}" in src or f"async def {s}" in src or s+" =" in src)

# Ensure no leftover dangling has_email block (the buggy duplicate)
dup = src.count('if has_email and not (')
ok("no duplicate has_email blocks", dup == 0, f"found {dup}")

# Ensure imports used
ok("imports asyncio for create_task", "import asyncio" in src)
ok("imports email_finder.find_emails_for_lab", "from app.email_finder import find_emails_for_lab" in src)

# ─── 2. Database schema sanity ──────────────────────────────────────
print("\n=== 2. DB SCHEMA ===")
db_src = open(f"{WD}/app/database.py").read()
ok("lead_emails table defined", "CREATE TABLE IF NOT EXISTS lead_emails" in db_src)
ok("lead_emails has UNIQUE(npi,email)", "UNIQUE(npi, email)" in db_src)
ok("lead_emails has idx on npi", "idx_emails_npi" in db_src)

# Insert column count must match my code's VALUES
insert_re = re.search(r"INSERT OR IGNORE INTO lead_emails\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)", src)
if insert_re:
    cols = [c.strip() for c in insert_re.group(1).split(",")]
    vals = insert_re.group(2).count("?")
    ok("INSERT cols vs ? markers", len(cols) == vals, f"cols={len(cols)} qmarks={vals}")
    # All cols must exist in table schema
    for col in cols:
        ok(f"col '{col}' in lead_emails schema", col in db_src)

# ─── 3. Auth bypass for /api/admin/labs ─────────────────────────────
print("\n=== 3. HUB ROUTING BYPASS ===")
hub_src = open(f"{WD}/app/hub_app.py").read()
ok("/admin/leads/api/admin/ in bypass", "/admin/leads/api/admin/" in hub_src)

# ─── 4. Template wiring ─────────────────────────────────────────────
print("\n=== 4. TEMPLATE ===")
tpl = open(f"{WD}/app/templates/index.html").read()
ok("template has nsHasEmailV2", 'id="nsHasEmailV2"' in tpl)
ok("template auto-runs search", "runNationalSearch, 2200" in tpl)
ok("template still has runNationalSearch fn", "async function runNationalSearch" in tpl)

# ─── 5. Live deployment ─────────────────────────────────────────────
print("\n=== 5. LIVE DEPLOY ===")
sha = subprocess.run(["git","rev-parse","HEAD"], cwd=WD, capture_output=True, text=True).stdout.strip()
print(f"  local HEAD: {sha[:12]}")
status = subprocess.run(["git","status","--porcelain"], cwd=WD, capture_output=True, text=True).stdout
ok("git tree clean", not status.strip(), repr(status[:200]))

with urllib.request.urlopen(f"{BASE}/buildz", timeout=20) as r:
    bm = json.loads(r.read().decode()).get("build_marker","")
print(f"  live build:  {bm[:12]}")
ok("live build matches local HEAD", bm == sha, f"{bm[:8]} vs {sha[:8]}")

# If not matching, push & wait
if bm != sha:
    print("\n  >>> pushing")
    subprocess.run(["git","push","origin","main"], cwd=WD)
    for i in range(80):
        time.sleep(8)
        try:
            with urllib.request.urlopen(f"{BASE}/buildz", timeout=15) as r:
                bm = json.loads(r.read().decode()).get("build_marker","")
                if bm == sha:
                    print(f"  build {bm[:8]} live after {i+1} polls"); break
        except Exception: pass

# ─── 6. Live functional behavior ────────────────────────────────────
print("\n=== 6. LIVE FUNCTIONAL ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=20).read()

# search default
with op.open(f"{BASE}/admin/leads/api/national-pull/search?has_email=false&limit=5", timeout=30) as r:
    d = json.loads(r.read().decode())
    ok("search default returns 13k+", d.get("matched",0) > 13000, f"matched={d.get('matched')}")

# specialties dropdown
with op.open(f"{BASE}/admin/leads/api/national-pull/specialties", timeout=30) as r:
    d = json.loads(r.read().decode())
    ok("specialties >=10", len(d.get("specialties",[])) >= 10, f"{len(d.get('specialties',[]))}")

# Lab enrich-status reachable (no 404)
try:
    with op.open(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=30) as r:
        d = json.loads(r.read().decode())
        ok("/labs/enrich-status reachable", "running" in d, json.dumps(d)[:200])
except urllib.error.HTTPError as e:
    ok("/labs/enrich-status reachable", False, f"HTTP {e.code}: {e.read().decode()[:200]}")

# Trigger small enrich job to validate end-to-end
print("\n=== 7. TRIGGER MICRO ENRICH (FL tier-A, limit=3) ===")
try:
    rq = urllib.request.Request(
        f"{BASE}/admin/leads/api/admin/labs/enrich-batch?state=FL&tier=A&limit=3",
        method="POST", headers={"Content-Type":"application/json"})
    with op.open(rq, timeout=30) as r:
        d = json.loads(r.read().decode())
        print("  trigger:", d)
        ok("trigger returns ok", d.get("ok") is True or d.get("running") is True)
except urllib.error.HTTPError as e:
    ok("enrich-batch trigger", False, f"HTTP {e.code}: {e.read().decode()[:200]}")

# Poll status up to 90s
done = False
for i in range(18):
    time.sleep(5)
    try:
        with op.open(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=20) as r:
            d = json.loads(r.read().decode())
            print(f"  poll {i+1}: running={d.get('running')} last={d.get('last_result')}")
            if not d.get("running") and d.get("last_result"):
                done = True; break
    except Exception as e:
        print(f"  poll {i+1}: err {e}")
ok("enrichment job finished within 90s", done)
if done and isinstance(d.get("last_result"), dict):
    lr = d["last_result"]
    print(f"  result: candidates={lr.get('candidates')} enriched_orgs={lr.get('enriched_orgs')} inserted={lr.get('inserted_emails')}")

# Re-search FL with has_email=true to see if any emails exist
with op.open(f"{BASE}/admin/leads/api/national-pull/search?state=FL&has_email=true&limit=10", timeout=30) as r:
    d = json.loads(r.read().decode())
    print(f"  FL has_email=true: matched={d.get('matched')}")
    for row in d.get('rows', [])[:3]:
        print(f"    - {row.get('Org Name')} | {row.get('DM Email') or row.get('Company Email')}")

# ─── Summary ────────────────────────────────────────────────────────
print("\n" + "="*60)
print(f"PASS: {len(PASS)}   FAIL: {len(FAIL)}")
print("="*60)
for n,d in FAIL: print(f"  FAIL: {n}  {d}")
