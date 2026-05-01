#!/usr/bin/env python3
"""Poll the search endpoint until the CSV starts populating."""
import time, urllib.request, json, sys
BASE = "https://medpharma-hub.onrender.com"
deadline = time.time() + 600
prev = -1
while time.time() < deadline:
    try:
        with urllib.request.urlopen(BASE + "/admin/leads/api/national-pull/status", timeout=20) as r:
            st = json.loads(r.read().decode())
        with urllib.request.urlopen(BASE + "/admin/leads/api/national-pull/specialties", timeout=20) as r:
            sp = json.loads(r.read().decode())
        latest = st.get("latest") or {}
        running = st.get("running")
        n_sp = len(sp.get("specialties") or [])
        row_count = latest.get("row_count")
        size = latest.get("size")
        ts = time.strftime("%H:%M:%S")
        if n_sp != prev or row_count or size:
            print(f"[{ts}] running={running} specialties={n_sp} row_count={row_count} csv_size={size} csv={latest.get('csv_path','')}")
            prev = n_sp
        if n_sp > 0:
            print("=== DATA AVAILABLE ===")
            print(json.dumps(sp.get("specialties", [])[:10], indent=2))
            sys.exit(0)
    except Exception as e:
        print(f"err: {e}")
    time.sleep(30)
print("TIMEOUT")
sys.exit(1)
