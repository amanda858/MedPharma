#!/usr/bin/env python3
"""Poll national-pull until CSV appears, then exercise search and report rows."""
import urllib.request, json, time, sys
BASE = "https://medpharma-hub.onrender.com"

def get(path):
    with urllib.request.urlopen(BASE + path, timeout=30) as r:
        return r.status, json.loads(r.read().decode("utf-8","replace"))

deadline = time.time() + 25 * 60
last = None
while time.time() < deadline:
    try:
        _, st = get("/admin/leads/api/national-pull/status")
        running = st.get("running")
        latest = st.get("latest") or {}
        rc = latest.get("row_count")
        msg = f"running={running} row_count={rc} csv={latest.get('csv_path','')[-60:]}"
        if msg != last:
            print(time.strftime("%H:%M:%S"), msg)
            last = msg
        if rc and int(rc) > 0:
            break
    except Exception as e:
        print(time.strftime("%H:%M:%S"), f"ERR {e}")
    time.sleep(20)

print("\n=== /specialties ===")
try:
    _, sp = get("/admin/leads/api/national-pull/specialties")
    print(json.dumps(sp, indent=2)[:1200])
except Exception as e:
    print("ERR", e)

print("\n=== /search?has_email=true&limit=3 ===")
try:
    _, sr = get("/admin/leads/api/national-pull/search?has_email=true&limit=3")
    print(f"total={sr.get('total')} matched={sr.get('matched')}")
    for r in sr.get("rows", [])[:3]:
        print("-", r.get("State"), "|", r.get("Org Name"), "|", r.get("DM Email") or r.get("Company Email"), "| heat", r.get("Heat Score"))
except Exception as e:
    print("ERR", e)
