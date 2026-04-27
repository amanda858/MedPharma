"""Live end-to-end verification of the unified hub + lead quality audit."""
import httpx, time, re, sys

BASE = "https://medpharma-hub.onrender.com"
print("="*70)
print("LIVE END-TO-END VERIFICATION  ::  " + BASE)
print("="*70)

issues = []

with httpx.Client(timeout=60, follow_redirects=False) as c:
    # 1. Single-link entry
    r = c.get(BASE + "/")
    print(f"\n[1] GET /  -> {r.status_code}  Location={r.headers.get('location','-')}")
    if r.status_code not in (301,302,307,308) or "/hub" not in r.headers.get('location',''):
        issues.append(f"root does not redirect to /hub (got {r.status_code} -> {r.headers.get('location')})")

    # 2. /hub serves login/hub HTML
    r = c.get(BASE + "/hub", follow_redirects=True)
    print(f"[2] GET /hub  -> {r.status_code}  bytes={len(r.content):,}")
    html = r.text
    checks = {
        "'Lead Source' label": "Lead Source" in html,
        "Dashboard tab": 'data-panel="dashboard"' in html,
        "Team Production tab": 'data-panel="production"' in html,
        "Documents tab": 'data-panel="files"' in html,
        "Lead Source tab": 'data-panel="leads"' in html,
        "Default = Dashboard active": 'class="hub-panel active" id="panel-dashboard"' in html,
        "Lead Source NOT active by default": 'class="hub-panel active" id="panel-leads"' not in html,
        "Hash deep-link wiring": "openFromHash" in html,
        "Embedded Lead Source iframe": '/admin/leads/' in html and 'leadsIframe' in html,
    }
    for k,v in checks.items():
        print(f"    {'OK ' if v else 'FAIL'}  {k}")
        if not v:
            issues.append(f"hub UI: {k} missing")

    # 3. Lead Source API auth allowlist
    r = c.get(BASE + "/admin/leads/api/prospect/specialties")
    print(f"\n[3] GET /admin/leads/api/prospect/specialties  -> {r.status_code}")
    if r.status_code == 200:
        specs = r.json()
        print(f"    specialties ({len(specs.get('specialties',specs)) if isinstance(specs,dict) else len(specs)}): {specs}")
    else:
        issues.append(f"specialties endpoint returned {r.status_code}")

    # 4. Run a real hunt
    print(f"\n[4] POST /admin/leads/api/prospect/bulk  state=FL specialty=clinical limit=10 new_only=true")
    r = c.post(BASE + "/admin/leads/api/prospect/bulk",
               json={"state":"FL","specialty":"clinical","limit":10,"new_only":True,"dm_only":True},
               timeout=60)
    print(f"    -> {r.status_code}")
    if r.status_code != 200:
        print(f"    body: {r.text[:400]}")
        issues.append(f"hunt POST failed: {r.status_code}")
        sys.exit(1)
    job = r.json().get("job_id")
    print(f"    job_id: {job}")

    rows = []
    dl = {}
    summary = {}
    for i in range(60):
        time.sleep(2)
        sr = c.get(BASE + f"/admin/leads/api/scrub/status/{job}")
        sj = sr.json()
        st = sj.get("status")
        if st in ("done","completed","ready","success"):
            rows = sj.get("preview") or sj.get("rows") or sj.get("daily_top_10") or []
            dl = sj.get("download") or {}
            summary = sj.get("summary") or {}
            print(f"    DONE after {(i+1)*2}s — rows={len(rows)}  status={st}")
            print(f"    download keys: {list(dl.keys())}")
            print(f"    summary: {summary}")
            break
        if st == "error":
            print(f"    ERROR: {sj.get('error')}")
            issues.append(f"hunt errored: {sj.get('error')}")
            sys.exit(1)
    else:
        issues.append("hunt timed out after 120s")

    # 5. Lead quality audit
    if rows:
        print(f"\n[5] LEAD QUALITY AUDIT  ({len(rows)} prospects)")
        BAD_PHONES = {"000-000-0000","555-555-5555","123-456-7890","000 000 0000","(000) 000-0000"}
        BAD_NAME_TOKENS = {"unknown","n/a","na","none","null","test","sample"}
        q = {"with_dm":0,"with_phone":0,"with_li":0,"with_fb":0,"with_ig":0,
             "with_hook":0,"placeholder_phone":0,"bad_name":0,"with_org":0}
        for r2 in rows:
            dm = (r2.get("Decision Maker") or r2.get("DM Name") or "").strip()
            org = (r2.get("Org Name") or r2.get("Organization") or "").strip()
            ph = (r2.get("Phone") or r2.get("DM Phone") or "").strip()
            li = (r2.get("LinkedIn") or r2.get("LinkedIn URL") or "").strip()
            fb = (r2.get("Facebook") or r2.get("Facebook URL") or "").strip()
            ig = (r2.get("Instagram") or r2.get("Instagram URL") or "").strip()
            hook = (r2.get("Hook") or r2.get("Personalized Hook") or "").strip()
            if org: q["with_org"] += 1
            if dm and dm.lower() not in BAD_NAME_TOKENS: q["with_dm"] += 1
            if ph: q["with_phone"] += 1
            if li and "linkedin.com" in li: q["with_li"] += 1
            if fb and "facebook.com" in fb: q["with_fb"] += 1
            if ig and ("instagram.com" in ig or "google.com" in ig): q["with_ig"] += 1
            if hook and len(hook) > 20: q["with_hook"] += 1
            if ph in BAD_PHONES: q["placeholder_phone"] += 1
            if dm and (not re.match(r"^[A-Za-z][A-Za-z\.\-\' ]{2,}$", dm) or dm.lower() in BAD_NAME_TOKENS):
                q["bad_name"] += 1
        n = len(rows)
        for k,v in q.items():
            pct = (v/n*100) if n else 0
            flag = ""
            if k in {"with_dm","with_org","with_phone","with_li","with_fb","with_ig","with_hook"} and pct < 80:
                flag = "  <-- LOW"
                issues.append(f"quality: {k} only {pct:.0f}%")
            if k in {"placeholder_phone","bad_name"} and v > 0:
                flag = "  <-- BAD"
                issues.append(f"quality: {v} rows have {k}")
            print(f"      {k:22s} {v:3d}/{n}  ({pct:5.1f}%){flag}")

        print(f"\n    Sample 5 prospects:")
        for i,r2 in enumerate(rows[:5],1):
            dm = r2.get("Decision Maker") or r2.get("DM Name") or "?"
            org = r2.get("Org Name") or r2.get("Organization") or "?"
            print(f"    {i}. {dm[:32]:32s} @ {org[:42]:42s}")
            print(f"       title={(r2.get('DM Title') or r2.get('Title') or '?')[:50]}  phone={r2.get('Phone','?')}")
            li = r2.get('LinkedIn') or r2.get('LinkedIn URL') or '-'
            fb = r2.get('Facebook') or r2.get('Facebook URL') or '-'
            ig = r2.get('Instagram') or r2.get('Instagram URL') or '-'
            print(f"       LI={li[:90]}")
            print(f"       FB={fb[:90]}")
            print(f"       IG={ig[:90]}")
            hook = r2.get('Hook') or r2.get('Personalized Hook') or '-'
            print(f"       hook=\"{hook[:100]}\"")

    # 6. Verify download links resolve
    print(f"\n[6] DOWNLOAD LINKS")
    for k, path in (dl.items() if isinstance(dl, dict) else []):
        # The hub UI prefixes with /admin/leads via withBase() — mirror that here
        if path.startswith("http"):
            url = path
        elif path.startswith("/admin/leads"):
            url = BASE + path
        else:
            url = BASE + "/admin/leads" + path
        try:
            h = c.get(url, follow_redirects=True, timeout=20)
            print(f"    {k:12s} -> {h.status_code}  {len(h.content):,} bytes  {url[:70]}")
            if h.status_code != 200:
                issues.append(f"download {k} returned {h.status_code}")
        except Exception as e:
            print(f"    {k:12s} ERROR {e}")
            issues.append(f"download {k} error: {e}")

    # 7. Team Production endpoints reachable (auth-protected, expect 401 unauth)
    print(f"\n[7] TEAM PRODUCTION ENDPOINTS")
    for ep in ["/hub/api/production",
               "/hub/api/production/report",
               "/hub/api/production/report/download",
               "/hub/api/files",
               "/hub/api/dashboard"]:
        rr = c.get(BASE + ep, follow_redirects=False)
        ok = rr.status_code in (200, 401)  # 401 = endpoint exists, just needs login
        flag = "OK " if ok else "FAIL"
        print(f"    {flag}  GET {ep:40s} -> {rr.status_code}")
        if not ok:
            issues.append(f"{ep} returned {rr.status_code}")

print("\n"+"="*70)
if issues:
    print(f"FOUND {len(issues)} ISSUES:")
    for i,iss in enumerate(issues,1):
        print(f"  {i}. {iss}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED")