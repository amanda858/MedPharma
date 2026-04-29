#!/usr/bin/env python3
"""Live diagnostic for national pull state."""
import urllib.request, json, subprocess
BASE = "https://medpharma-hub.onrender.com"

print("=== git log -10 ===")
print(subprocess.run(["git","-C","/workspaces/CVOPro","log","--oneline","-10"],
                     capture_output=True, text=True).stdout)

for path in ["/admin/leads/api/national-pull/status",
             "/admin/leads/api/national-pull/specialties",
             "/admin/leads/api/national-pull/search?has_email=false&limit=2"]:
    print(f"\n=== {path} ===")
    try:
        with urllib.request.urlopen(BASE + path, timeout=25) as r:
            body = r.read().decode("utf-8","replace")
            print(f"HTTP {r.status}")
            try:
                print(json.dumps(json.loads(body), indent=2)[:1500])
            except Exception:
                print(body[:1500])
    except Exception as e:
        print(f"ERR {type(e).__name__}: {e}")
