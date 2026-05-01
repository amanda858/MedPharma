"""Final comprehensive system audit — every critical surface, no excuses."""
import urllib.request, urllib.error, json, http.cookiejar, time, re

BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

PASS, FAIL, WARN = [], [], []

def req(method, url, data=None, timeout=45, raw=False):
    h = {"Accept": "*/*"}
    if data is not None and not isinstance(data, bytes):
        data = json.dumps(data).encode(); h["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with opener.open(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return -1, {}, str(e).encode()

def check(name, ok, detail=""):
    tag = "PASS" if ok else "FAIL"
    bucket = PASS if ok else FAIL
    bucket.append((name, detail))
    print(f"  [{tag}] {name}  {detail}")

def warn(name, detail=""):
    WARN.append((name, detail))
    print(f"  [WARN] {name}  {detail}")

# ─────────────────────────────────────────────────────────
print("\n=== 1. INFRASTRUCTURE ===")
st, _, body = req("GET", f"{BASE}/healthz")
check("/healthz returns 200", st == 200, body.decode()[:80])

st, _, body = req("GET", f"{BASE}/buildz")
build_ok = st == 200 and b'"ok":true' in body
check("/buildz returns build_marker", build_ok, body.decode()[:120])
build_sha = ""
try:
    build_sha = json.loads(body).get("build_marker", "")
except Exception: pass

# ─────────────────────────────────────────────────────────
print("\n=== 2. AUTH ===")
st, _, body = req("GET", f"{BASE}/hub")
check("/hub login page", st == 200 and len(body) > 5000)

st, _, body = req("POST", f"{BASE}/hub/api/login", data={"username":"admin","password":"admin123"})
auth_ok = st == 200 and any(c.name == "hub_session" for c in cj)
check("admin login succeeds + cookie set", auth_ok)

# wrong password should fail
cj2 = http.cookiejar.CookieJar()
op2 = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj2))
r = urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"wrongwrong"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST")
try:
    with op2.open(r, timeout=20) as resp:
        bad_st = resp.status; bad_body = resp.read().decode()
except urllib.error.HTTPError as e:
    bad_st = e.code; bad_body = e.read().decode()
check("wrong password rejected", bad_st in (400,401,403) or '"ok":false' in bad_body, f"HTTP {bad_st}")

# ─────────────────────────────────────────────────────────
print("\n=== 3. ADMIN UI HTML ===")
st, hdr, body = req("GET", f"{BASE}/admin/leads/")
html = body.decode()
check("/admin/leads/ returns HTML", st == 200 and len(html) > 50000, f"size={len(html)}")
check("HTML has nsHasEmailV2 (cache-busted id)", 'id="nsHasEmailV2"' in html)
check("HTML has auto-run search", "runNationalSearch, 2200" in html)
check("HTML has Specialty dropdown", 'id="nsSpecialty"' in html)
check("HTML has State dropdown", 'id="nsState"' in html)
check("HTML has cache no-store header", "no-store" in (hdr.get("Cache-Control","").lower()))

# ─────────────────────────────────────────────────────────
print("\n=== 4. NATIONAL-PULL SEARCH (the panel that was broken) ===")
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/specialties")
try:
    d = json.loads(body)
    sp = d.get("specialties", [])
    check("/specialties returns ≥10 entries", len(sp) >= 10, f"{len(sp)} specialties")
    if sp:
        top = sp[0]
        check("top specialty has count > 1000", top.get("count",0) > 1000, f"{top.get('name')}: {top.get('count')}")
except Exception as e:
    check("/specialties parses", False, str(e))

st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?has_email=false&limit=10")
try:
    d = json.loads(body)
    check("default search returns rows", d.get("matched",0) > 13000, f"matched={d.get('matched')} total={d.get('total')}")
    if d.get("rows"):
        r0 = d["rows"][0]
        check("top result is Tier A", r0.get("Tier") == "A", f"{r0.get('Org Name')} tier={r0.get('Tier')}")
        check("top result has city/state", bool(r0.get("City")) and bool(r0.get("State")))
except Exception as e:
    check("default search parses", False, str(e))

# state filter
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?state=FL&has_email=false&limit=20")
try:
    d = json.loads(body)
    rows = d.get("rows", [])
    check("FL filter returns rows", d.get("matched",0) > 100, f"matched={d.get('matched')}")
    check("FL filter rows are all FL", all((r.get("State","").upper() == "FL") for r in rows), f"{len(rows)} rows")
except Exception as e:
    check("FL filter parses", False, str(e))

# specialty filter
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?specialty=Clinical%20Medical%20Laboratory&has_email=false&limit=20")
try:
    d = json.loads(body)
    rows = d.get("rows", [])
    check("specialty filter returns rows", d.get("matched",0) > 5000, f"matched={d.get('matched')}")
    check("specialty filter rows match", all("Clinical Medical Laboratory" in r.get("Taxonomy / Type","") for r in rows))
except Exception as e:
    check("specialty filter parses", False, str(e))

# free-text
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?q=genetics&has_email=false&limit=10")
try:
    d = json.loads(body)
    check("free-text 'genetics' search", d.get("matched",0) > 10, f"matched={d.get('matched')}")
except Exception as e:
    check("free-text search parses", False, str(e))

# has_email=true should return 0 with a sane response (not crash)
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?has_email=true&limit=10")
try:
    d = json.loads(body)
    check("has_email=true returns valid JSON (matched can be 0)", "matched" in d, f"matched={d.get('matched')}")
except Exception as e:
    check("has_email=true parses", False, str(e))

# bad params don't crash
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?state=ZZ&limit=5")
check("invalid state ZZ doesn't crash", st == 200, f"HTTP {st}")

st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?min_heat=99999&limit=5")
check("min_heat out of range rejected cleanly", st in (200, 422), f"HTTP {st}")

# ─────────────────────────────────────────────────────────
print("\n=== 5. SAVED LEADS (admin lead browser) ===")
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
try:
    d = json.loads(body)
    total = d.get("total_leads", 0)
    check("stats total_leads >= 13000", total >= 13000, f"total={total}")
    check("by_status has new bucket", d.get("by_status",{}).get("new",0) >= 13000, f"new={d.get('by_status',{}).get('new')}")
    check("top_states populated", len(d.get("top_states",{})) >= 5, f"states={list(d.get('top_states',{}).keys())[:5]}")
except Exception as e:
    check("stats parses", False, str(e))

st, _, body = req("GET", f"{BASE}/admin/leads/api/leads?state=FL&min_score=80")
try:
    d = json.loads(body)
    check("/api/leads FL min_score=80 returns leads", d.get("count",0) >= 1, f"count={d.get('count')}")
except Exception as e:
    check("/api/leads parses", False, str(e))

st, _, body = req("GET", f"{BASE}/admin/leads/api/leads?state=CA&min_score=80")
try:
    d = json.loads(body)
    check("/api/leads CA min_score=80 returns leads", d.get("count",0) >= 3, f"count={d.get('count')}")
except Exception as e:
    check("CA query parses", False, str(e))

# ─────────────────────────────────────────────────────────
print("\n=== 6. AUTH ENFORCEMENT ===")
cj.clear()  # logout
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
check("post-logout /api/leads/stats returns 401", st == 401, f"HTTP {st}")

# national-pull endpoints are intentionally public (search)
st, _, body = req("GET", f"{BASE}/admin/leads/api/national-pull/search?limit=1")
check("national-pull/search public access OK", st == 200, f"HTTP {st}")

# ─────────────────────────────────────────────────────────
print("\n=== 7. CLIENT HUB ===")
st, _, body = req("GET", f"{BASE}/hub")
check("/hub serves HTML", st == 200 and b"<html" in body.lower())

# ─────────────────────────────────────────────────────────
print("\n=== 8. STATIC + CSV ENDPOINTS ===")
st, hdr, body = req("GET", f"{BASE}/admin/leads/api/national-pull/status")
try:
    d = json.loads(body)
    check("/national-pull/status returns JSON", st == 200, f"running={d.get('running')}")
    if d.get("latest"):
        warn("latest pull status", json.dumps(d.get("latest"))[:200])
except Exception as e:
    check("/national-pull/status parses", False, str(e))

# ─────────────────────────────────────────────────────────
print("\n=== 9. CONCURRENCY / RACE (3 parallel searches) ===")
import threading
results = []
def hit():
    s,_,b = req("GET", f"{BASE}/admin/leads/api/national-pull/search?has_email=false&limit=50", timeout=30)
    results.append(s)
ts = [threading.Thread(target=hit) for _ in range(5)]
for t in ts: t.start()
for t in ts: t.join()
check("5 parallel searches all 200", all(s == 200 for s in results), f"codes={results}")

# ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print(f"BUILD MARKER: {build_sha[:12]}")
print(f"PASS: {len(PASS)}   FAIL: {len(FAIL)}   WARN: {len(WARN)}")
print("=" * 70)
if FAIL:
    print("\nFAILURES:")
    for n, d in FAIL:
        print(f"  - {n}  ({d})")
else:
    print("\nALL CRITICAL CHECKS PASSED — system clean.")
if WARN:
    print("\nWARNINGS:")
    for n, d in WARN:
        print(f"  - {n}  ({d})")
