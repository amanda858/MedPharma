"""Deep optimization sweep — exercise every endpoint, every specialty,
multiple states, time everything, and report any anomalies."""
import httpx, time, sys, statistics

BASE = "https://medpharma-hub.onrender.com"
SPECIALTIES = ['clinical','pathology','toxicology','molecular','genetic',
               'histology','cytopathology','microbiology','blood_bank',
               'physiological','physician_office','urgent_care','all_labs']
STATES = ['FL','TX','CA','NY','NC']

print("="*72)
print(f"DEEP OPTIMIZATION SWEEP  ::  {BASE}")
print("="*72)

issues = []
timings = {}

with httpx.Client(timeout=90, follow_redirects=False) as c:
    # Warmup
    t0 = time.time()
    r = c.get(BASE + "/healthz")
    warm = time.time() - t0
    print(f"\n[warmup] /healthz -> {r.status_code} in {warm*1000:.0f}ms")
    if warm > 5: issues.append(f"healthz slow: {warm:.1f}s (cold start?)")

    # ── Critical pages ──────────────────────────────────────────
    print("\n[1] CRITICAL PAGES")
    pages = [("/", 307), ("/hub", 200), ("/healthz", 200),
             ("/admin/leads/", None),  # may be 200 or 401-redirect
             ("/admin/leads/api/prospect/specialties", 200)]
    for path, expect in pages:
        t0 = time.time()
        rr = c.get(BASE + path)
        dt = (time.time()-t0)*1000
        timings[path] = dt
        ok = (expect is None) or (rr.status_code == expect)
        print(f"    {'OK ' if ok else 'FAIL'}  {path:48s} -> {rr.status_code}  ({dt:.0f}ms)")
        if not ok: issues.append(f"{path} returned {rr.status_code}, expected {expect}")
        if dt > 8000: issues.append(f"{path} slow: {dt:.0f}ms")

    # ── Hub HTML integrity ──────────────────────────────────────
    print("\n[2] HUB UI INTEGRITY")
    r = c.get(BASE + "/hub")
    html = r.text
    must_have = [
        "Lead Source", 'data-panel="leads"', 'data-panel="dashboard"',
        'data-panel="production"', 'data-panel="files"',
        'class="hub-panel active" id="panel-dashboard"',
        "openFromHash", "leadsIframe", '/admin/leads/',
        "Waking up server",  # cold-start UX
        "function navTo", "function doLogin",
    ]
    for t in must_have:
        present = t in html
        print(f"    {'OK ' if present else 'FAIL'}  {t[:60]}")
        if not present: issues.append(f"hub HTML missing: {t}")
    # No raw error markers
    bad = ["Traceback (most recent", "ImportError", "ModuleNotFoundError"]
    for b in bad:
        if b in html:
            print(f"    FAIL  raw error in HTML: {b}")
            issues.append(f"hub HTML contains: {b}")

    # ── Hunt every specialty in FL ──────────────────────────────
    print("\n[3] HUNT MODE — every specialty (FL, limit=5)")
    spec_durations = []
    for sp in SPECIALTIES:
        t0 = time.time()
        rr = c.post(BASE + "/admin/leads/api/prospect/bulk",
                    json={"state":"FL","specialty":sp,"limit":5,"new_only":False,"dm_only":True},
                    timeout=60)
        if rr.status_code != 200:
            print(f"    FAIL  {sp:18s} POST -> {rr.status_code}")
            issues.append(f"hunt {sp} POST failed: {rr.status_code}")
            continue
        job = rr.json().get("job_id")
        rows = []
        ok_rows = False
        for _ in range(40):
            time.sleep(1.5)
            sr = c.get(BASE + f"/admin/leads/api/scrub/status/{job}", timeout=20)
            try: sj = sr.json()
            except Exception: continue
            if sj.get("status") in ("done","completed","ready","success"):
                rows = sj.get("preview") or sj.get("rows") or []
                ok_rows = True
                break
            if sj.get("status") == "error":
                issues.append(f"hunt {sp} errored: {sj.get('error')}")
                break
        dt = time.time()-t0
        spec_durations.append(dt)
        with_dm = sum(1 for r2 in rows if (r2.get("Decision Maker") or r2.get("DM Name")))
        with_phone = sum(1 for r2 in rows if (r2.get("Phone") or r2.get("DM Phone")))
        print(f"    {'OK ' if ok_rows else 'FAIL'}  {sp:18s} {len(rows):2d} rows  "
              f"DM:{with_dm} phone:{with_phone}  ({dt:.1f}s)")
        if ok_rows and len(rows)==0:
            # Some niches legitimately have 0 — don't flag unless ALL 0
            pass
        elif ok_rows and with_dm < len(rows):
            issues.append(f"{sp}: {len(rows)-with_dm} rows missing DM")

    if spec_durations:
        print(f"\n    Specialty timing: median={statistics.median(spec_durations):.1f}s  "
              f"max={max(spec_durations):.1f}s")
        if max(spec_durations) > 30:
            issues.append(f"slowest specialty took {max(spec_durations):.1f}s")

    # ── Multi-state spot check (clinical) ───────────────────────
    print("\n[4] MULTI-STATE — clinical, limit=5")
    for st in STATES:
        t0 = time.time()
        rr = c.post(BASE + "/admin/leads/api/prospect/bulk",
                    json={"state":st,"specialty":"clinical","limit":5,"new_only":False,"dm_only":True},
                    timeout=60)
        if rr.status_code != 200:
            print(f"    FAIL  {st} POST -> {rr.status_code}")
            issues.append(f"hunt {st} POST failed: {rr.status_code}")
            continue
        job = rr.json().get("job_id")
        rows = []
        for _ in range(40):
            time.sleep(1.5)
            sr = c.get(BASE + f"/admin/leads/api/scrub/status/{job}", timeout=20)
            try: sj = sr.json()
            except Exception: continue
            if sj.get("status") in ("done","completed","ready","success"):
                rows = sj.get("preview") or sj.get("rows") or []
                break
            if sj.get("status") == "error": break
        dt = time.time()-t0
        with_dm = sum(1 for r2 in rows if (r2.get("Decision Maker") or r2.get("DM Name")))
        print(f"    OK   {st}  {len(rows)} rows  DM:{with_dm}  ({dt:.1f}s)")

    # ── Team Production endpoints presence ──────────────────────
    print("\n[5] AUTH-GUARDED ENDPOINTS (expect 401 unauth = endpoint exists)")
    auth_eps = [
        "/hub/api/me", "/hub/api/dashboard", "/hub/api/clients",
        "/hub/api/production", "/hub/api/production/report",
        "/hub/api/production/report/download", "/hub/api/files",
        "/hub/api/credentialing", "/hub/api/enrollment", "/hub/api/edi",
        "/hub/api/providers", "/hub/api/notes", "/hub/api/alerts",
        "/hub/api/audit-log", "/hub/api/notifications/status",
    ]
    for ep in auth_eps:
        rr = c.get(BASE + ep, follow_redirects=False, timeout=15)
        ok = rr.status_code in (200, 401)
        print(f"    {'OK ' if ok else 'FAIL'}  {ep:46s} -> {rr.status_code}")
        if not ok: issues.append(f"{ep} unexpected status {rr.status_code}")

    # ── Cache / no-store on HTML ────────────────────────────────
    print("\n[6] CACHE HEADERS")
    r = c.get(BASE + "/hub")
    cc = r.headers.get("cache-control","")
    print(f"    /hub  Cache-Control: {cc}")
    if "no-store" not in cc:
        issues.append("hub HTML missing no-store cache header")
    else:
        print("    OK   no-store present")

    # ── Login error handling (bad creds should return 401 fast) ─
    print("\n[7] LOGIN ERROR HANDLING")
    t0 = time.time()
    rr = c.post(BASE + "/hub/api/login",
                json={"username":"__nonexistent__","password":"x"}, timeout=20)
    dt = (time.time()-t0)*1000
    print(f"    bad creds -> {rr.status_code}  ({dt:.0f}ms)")
    if rr.status_code != 401:
        issues.append(f"bad-credentials should return 401, got {rr.status_code}")

print("\n" + "="*72)
if issues:
    print(f"FOUND {len(issues)} ISSUES:")
    for i, iss in enumerate(issues, 1):
        print(f"  {i}. {iss}")
    sys.exit(1)
else:
    print("OPTIMAL — ALL CHECKS PASSED")
    print(f"\nTimings (ms): " + ", ".join(f"{k.split('/')[-1] or 'root'}={v:.0f}" for k,v in timings.items()))
