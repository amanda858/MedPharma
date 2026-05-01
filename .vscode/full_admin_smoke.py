"""Full end-to-end admin login + UI smoke test."""
import urllib.request, urllib.error, json, http.cookiejar, time

BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def req(method, url, data=None, headers=None, timeout=60):
    h = {"Accept": "*/*"}
    if headers: h.update(headers)
    if data is not None and not isinstance(data, bytes):
        data = json.dumps(data).encode("utf-8")
        h["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with opener.open(r, timeout=timeout) as resp:
            return resp.status, resp.headers, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read().decode("utf-8", errors="replace")

results = {}

# 1. healthz
print("[1] /healthz")
st, _, body = req("GET", f"{BASE}/healthz")
print(f"   HTTP {st}  {body[:120]}")
results["healthz"] = st == 200

# 2. login page renders
print("\n[2] /hub login page")
st, _, body = req("GET", f"{BASE}/hub")
print(f"   HTTP {st}  size={len(body)}  has-form={'login' in body.lower() or 'password' in body.lower()}")
results["login_page"] = st == 200

# 3. login with admin/admin123
print("\n[3] POST /hub/api/login admin/admin123")
st, _, body = req("POST", f"{BASE}/hub/api/login", data={"username":"admin","password":"admin123"})
print(f"   HTTP {st}  {body[:200]}")
auth_cookie = next((c for c in cj if c.name == "hub_session"), None)
print(f"   cookie set: {auth_cookie is not None}  name={auth_cookie.name if auth_cookie else None}")
results["login"] = st == 200 and auth_cookie is not None

# 4. /admin/leads/ page (the actual UI)
print("\n[4] GET /admin/leads/  (authed UI)")
st, h, body = req("GET", f"{BASE}/admin/leads/")
print(f"   HTTP {st}  size={len(body)}  ctype={h.get('Content-Type')}")
print(f"   has-search-ui={'search' in body.lower()}  has-leads={'leads' in body.lower()}")
results["leads_ui"] = st == 200 and len(body) > 5000

# 5. stats endpoint
print("\n[5] GET /admin/leads/api/leads/stats")
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
print(f"   HTTP {st}  {body[:400]}")
try:
    d = json.loads(body)
    total = d.get("total_leads", 0)
    print(f"   total_leads={total}")
    results["stats"] = st == 200 and total >= 13000
    results["total_leads"] = total
except Exception:
    results["stats"] = False

# 6. FL Tier A search
print("\n[6] GET /admin/leads/api/leads?state=FL&min_score=80")
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads?state=FL&min_score=80")
try:
    d = json.loads(body)
    leads = d.get("leads", [])
    print(f"   HTTP {st}  count={d.get('count')}  returned={len(leads)}")
    for l in leads[:5]:
        print(f"     - {l.get('organization_name')} | {l.get('city')}, {l.get('state')} | score={l.get('lead_score')} | tags={l.get('tags')}")
    results["fl_tier_a"] = st == 200 and len(leads) >= 1
except Exception as e:
    print(f"   parse err {e}")
    results["fl_tier_a"] = False

# 7. CA Tier A
print("\n[7] GET /admin/leads/api/leads?state=CA&min_score=80")
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads?state=CA&min_score=80")
try:
    d = json.loads(body)
    leads = d.get("leads", [])
    print(f"   HTTP {st}  returned={len(leads)}")
    for l in leads[:3]:
        print(f"     - {l.get('organization_name')} | {l.get('city')}, {l.get('state')} | score={l.get('lead_score')}")
    results["ca_tier_a"] = st == 200
except Exception:
    results["ca_tier_a"] = False

# 8. Tier A any-state aggregate (just count)
print("\n[8] GET /admin/leads/api/leads?min_score=80  (count tier A across US)")
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads?min_score=80")
try:
    d = json.loads(body)
    leads = d.get("leads", [])
    rule_intercept = [l for l in leads if (l.get("source") or "") == "rule-intercept"]
    by_state = {}
    for l in rule_intercept:
        s = l.get("state") or "?"
        by_state[s] = by_state.get(s, 0) + 1
    print(f"   HTTP {st}  total tier-A leads returned: {len(leads)}  rule-intercept: {len(rule_intercept)}")
    print(f"   tier-A by state (top 10):")
    for s, c in sorted(by_state.items(), key=lambda kv: -kv[1])[:10]:
        print(f"     {s}: {c}")
    results["tier_a_total"] = len(rule_intercept)
except Exception as e:
    print(f"   err {e}")

# 9. logout (sanity)
print("\n[9] post-logout 401 sanity")
cj.clear()
st, _, body = req("GET", f"{BASE}/admin/leads/api/leads/stats")
print(f"   HTTP {st}  (expect 401 or redirect)")

# Summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
for k, v in results.items():
    print(f"  {k}: {v}")
fail = [k for k, v in results.items() if v is False]
if fail:
    print(f"\nFAILED: {fail}")
else:
    print("\nALL CORE CHECKS PASSED")
