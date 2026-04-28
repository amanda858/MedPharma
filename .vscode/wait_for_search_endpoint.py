#!/usr/bin/env python3
"""Wait for the search endpoints to be live on Render."""
import time, sys, urllib.request, json

BASE = "https://medpharma-hub.onrender.com"
URL = f"{BASE}/admin/leads/api/national-pull/specialties"

deadline = time.time() + 360
attempt = 0
while time.time() < deadline:
    attempt += 1
    try:
        req = urllib.request.Request(URL, headers={"User-Agent": "deploy-poll"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
            if r.status == 200:
                try:
                    data = json.loads(body)
                    n = len(data.get("specialties") or [])
                    print(f"[{attempt}] LIVE - status 200, {n} specialties available")
                    sys.exit(0)
                except Exception:
                    pass
            print(f"[{attempt}] status={r.status} body[:120]={body[:120]}")
    except Exception as e:
        print(f"[{attempt}] {type(e).__name__}: {e}")
    time.sleep(15)
print("TIMEOUT after 6 min")
sys.exit(1)
