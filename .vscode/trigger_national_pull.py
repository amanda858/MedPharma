#!/usr/bin/env python3
import urllib.request, json
BASE = "https://medpharma-hub.onrender.com"
req = urllib.request.Request(BASE + "/admin/leads/api/national-pull/run", method="POST", data=b"")
try:
    with urllib.request.urlopen(req, timeout=30) as r:
        print(f"HTTP {r.status}")
        print(json.dumps(json.loads(r.read().decode()), indent=2))
except Exception as e:
    print(f"ERROR: {e}")
