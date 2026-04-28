#!/usr/bin/env python3
import urllib.request, json
BASE = "https://medpharma-hub.onrender.com"
for path in ["/admin/leads/api/national-pull/status", "/admin/leads/api/national-pull/specialties"]:
    try:
        with urllib.request.urlopen(BASE + path, timeout=20) as r:
            print(f"=== {path} (HTTP {r.status}) ===")
            print(json.dumps(json.loads(r.read().decode()), indent=2)[:1500])
    except Exception as e:
        print(f"=== {path} ERROR: {e} ===")
    print()
