#!/usr/bin/env python3
"""Wait until live deploy reflects expected commit, then run probe."""
import urllib.request, json, time, subprocess, sys

EXPECTED = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd="/workspaces/CVOPro").decode().strip()
print(f"expected commit: {EXPECTED[:8]}")

BASE = "https://medpharma-hub.onrender.com"
for i in range(40):
    try:
        r = urllib.request.urlopen(f"{BASE}/buildz", timeout=30)
        info = json.loads(r.read())
        sha = info.get("build_marker") or ""
        print(f"  {i}: live build_marker={sha[:12]}")
        if sha.startswith(EXPECTED[:8]):
            print("DEPLOY MATCHES"); sys.exit(0)
    except Exception as e:
        print(f"  {i}: err {e}")
    time.sleep(15)

print("Timed out waiting for deploy"); sys.exit(1)
