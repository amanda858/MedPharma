#!/usr/bin/env python3
"""Run a real hunt and inspect output rows including backup-person fallback."""
import json, time, urllib.request, urllib.error, http.cookiejar, sys
BASE = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

# Wake server (cold start tolerant)
print("waking server...")
for i in range(10):
    try:
        op.open(f"{BASE}/healthz", timeout=120).read(); print("  awake"); break
    except Exception as e:
        print(f"  wake try {i}: {e}"); time.sleep(15)

# Login (cold start may delay)
print("login...")
for i in range(5):
    try:
        data = json.dumps({"username":"admin","password":"admin123"}).encode()
        op.open(urllib.request.Request(f"{BASE}/hub/api/login", data=data, headers={"Content-Type":"application/json"}, method="POST"), timeout=120).read()
        print("  logged in"); break
    except Exception as e:
        print(f"  login try {i}: {e}"); time.sleep(10)
else:
    print("LOGIN FAILED"); sys.exit(1)

# Start hunt
body = json.dumps({"state":"FL","specialty":"clinical","limit":3,"new_only":False,"dm_only":True}).encode()
r = op.open(urllib.request.Request(f"{BASE}/admin/leads/api/prospect/bulk", data=body, headers={"Content-Type":"application/json"}, method="POST"), timeout=120)
job = json.loads(r.read())
job_id = job.get("job_id")
print("job_id:", job_id)

# Poll up to 5 minutes with longer per-poll timeout
final = None
for i in range(60):
    try:
        r = op.open(f"{BASE}/admin/leads/api/scrub/status/{job_id}", timeout=60)
        s = json.loads(r.read())
    except Exception as e:
        print(f"  poll {i}: err {e}"); time.sleep(5); continue
    st = s.get("status")
    print(f"  poll {i}: {st}  progress={s.get('progress')}")
    if st in ("done","completed","ready","error","failed"):
        final = s; break
    time.sleep(5)

if not final:
    print("TIMEOUT after 5 minutes"); sys.exit(1)

print("\nFINAL STATUS:", final.get("status"))
preview = final.get("preview") or final.get("rows") or []
print("rows:", len(preview))

KEYS = ("Decision Maker","DM Title","Org Name","Address","City","State","ZIP",
        "Phone","Direct Line","DM Email","Heat Score","Tier",
        "LinkedIn URL","LinkedIn Match Type","LinkedIn Company Page","LinkedIn Other Employees",
        "Facebook URL","Instagram URL",
        "Backup Contact","Backup Title","Backup Phone","Backup NPI","Backup LinkedIn",
        "Personalized Hook")

for i, row in enumerate(preview[:5]):
    print(f"\n--- row {i+1} ---")
    for k in KEYS:
        v = row.get(k)
        if v: print(f"  {k}: {v}")
