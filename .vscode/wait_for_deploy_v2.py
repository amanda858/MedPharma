#!/usr/bin/env python3
"""Wait until Render serves the latest commit, then verify search endpoints."""
import time, sys, urllib.request, json, subprocess

BASE = "https://medpharma-hub.onrender.com"

def head_sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd="/workspaces/CVOPro").decode().strip()

def fetch(path: str):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": "deploy-poll"})
    with urllib.request.urlopen(req, timeout=20) as r:
        body = r.read().decode("utf-8", "replace")
        return r.status, body

def remote_sha() -> str:
    try:
        s, b = fetch("/buildz")
        if s == 200:
            d = json.loads(b)
            return (d.get("build_marker") or d.get("BUILD_MARKER") or "")[:40]
    except Exception:
        pass
    return ""

want = head_sha()
print(f"target HEAD={want[:10]}")
deadline = time.time() + 480
attempt = 0
last = ""
while time.time() < deadline:
    attempt += 1
    cur = remote_sha()
    matches = cur and (cur == want or want.startswith(cur) or cur.startswith(want[:10]))
    if cur != last:
        print(f"[{attempt}] remote={cur[:10] or '?'} match={bool(matches)}")
        last = cur
    if matches:
        print("=== DEPLOY LIVE ===")
        # Now hit the endpoints
        for path in ["/admin/leads/api/national-pull/status", "/admin/leads/api/national-pull/specialties"]:
            try:
                s, b = fetch(path)
                print(f"--- {path} (HTTP {s}) ---")
                print(json.dumps(json.loads(b), indent=2)[:1000])
            except Exception as e:
                print(f"--- {path} ERR {e} ---")
        sys.exit(0)
    time.sleep(15)
print("TIMEOUT after 8 min")
sys.exit(1)
