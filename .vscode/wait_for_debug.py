#!/usr/bin/env python3
"""Wait for /debug endpoint to come up after deploy, then dump it."""
import time, sys, urllib.request, json
URL = "https://medpharma-hub.onrender.com/admin/leads/api/national-pull/debug"
deadline = time.time() + 360
attempt = 0
while time.time() < deadline:
    attempt += 1
    try:
        with urllib.request.urlopen(URL, timeout=20) as r:
            body = r.read().decode("utf-8", "replace")
            if r.status == 200:
                try:
                    data = json.loads(body)
                    if "detail" not in data:
                        print(f"[attempt {attempt}] DEPLOY LIVE")
                        print(json.dumps(data, indent=2))
                        sys.exit(0)
                except Exception:
                    pass
            print(f"[{attempt}] HTTP {r.status} {body[:120]}")
    except Exception as e:
        print(f"[{attempt}] {type(e).__name__}: {e}")
    time.sleep(20)
print("TIMEOUT")
sys.exit(1)
