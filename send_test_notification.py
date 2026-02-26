#!/usr/bin/env python3
"""Send test SMS and email notification via MedPharma Hub API."""

import urllib.request
import urllib.error
import json
import sys
import os

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_output.txt")

def log(msg):
    print(msg, flush=True)
    with open(OUTPUT_FILE, "a") as f:
        f.write(msg + "\n")

# Clear previous output
with open(OUTPUT_FILE, "w") as f:
    f.write("")

BASE = "https://medpharma-hub.onrender.com"
TIMEOUT = 120

# Step 1: Login
log("=== Step 1: Logging in... ===")
login_data = json.dumps({"username": "eric", "password": "admin"}).encode()
login_req = urllib.request.Request(
    f"{BASE}/hub/api/login",
    data=login_data,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    login_resp = urllib.request.urlopen(login_req, timeout=TIMEOUT)
except urllib.error.HTTPError as e:
    body = e.read().decode()
    log(f"Login failed: HTTP {e.code} — {body}")
    sys.exit(1)
except Exception as e:
    log(f"Login request error: {e}")
    sys.exit(1)

# Extract hub_session cookie
cookie_header = login_resp.headers.get_all("Set-Cookie") or []
hub_session = None
for c in cookie_header:
    for part in c.split(";"):
        part = part.strip()
        if part.startswith("hub_session="):
            hub_session = part.split("=", 1)[1]
            break
    if hub_session:
        break

login_body = json.loads(login_resp.read().decode())
log(f"Login response: {json.dumps(login_body, indent=2)}")

if not hub_session:
    log("ERROR: No hub_session cookie found in response headers.")
    log(f"All Set-Cookie headers: {cookie_header}")
    sys.exit(1)

log(f"hub_session cookie: {hub_session[:20]}...")

# Step 2: Send test notification
log("\n=== Step 2: Sending test notification... ===")
test_req = urllib.request.Request(
    f"{BASE}/hub/api/notifications/test",
    data=b"",
    headers={
        "Cookie": f"hub_session={hub_session}",
        "Content-Type": "application/json",
    },
    method="POST",
)

try:
    test_resp = urllib.request.urlopen(test_req, timeout=TIMEOUT)
    result = json.loads(test_resp.read().decode())
    log(f"\n=== Test Notification Response ===")
    log(json.dumps(result, indent=2))
except urllib.error.HTTPError as e:
    body = e.read().decode()
    log(f"Test notification failed: HTTP {e.code}")
    try:
        log(json.dumps(json.loads(body), indent=2))
    except Exception:
        log(body)
except Exception as e:
    log(f"Test notification request error: {e}")
    sys.exit(1)

log("\n=== DONE ===")
