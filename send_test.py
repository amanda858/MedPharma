#!/usr/bin/env python3
"""Quick script to trigger a test notification on the live Render site.
Run: python3 send_test.py
"""
import urllib.request
import urllib.parse
import json
import sys

SITE = "https://medpharma-hub.onrender.com"

print("=== Logging into MedPharma Hub ===")
login_data = json.dumps({"username": "eric", "password": "admin"}).encode()
req = urllib.request.Request(
    f"{SITE}/hub/api/login",
    data=login_data,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    resp = urllib.request.urlopen(req, timeout=30)
except urllib.error.HTTPError as e:
    print(f"Login failed: HTTP {e.code} — {e.read().decode()[:200]}")
    sys.exit(1)

# Extract session cookie
cookie = None
for header in resp.headers.get_all("Set-Cookie") or []:
    if "hub_session" in header:
        cookie = header.split("hub_session=")[1].split(";")[0]
        break

if not cookie:
    print("Login succeeded but no session cookie returned")
    print(f"Response: {resp.read().decode()[:200]}")
    sys.exit(1)

print(f"Logged in! Cookie: {cookie[:12]}...")

# Send test notification
print("\n=== Sending Test Notification (SMS + Email) ===")
test_req = urllib.request.Request(
    f"{SITE}/hub/api/notifications/test",
    data=b"",
    headers={"Cookie": f"hub_session={cookie}"},
    method="POST",
)

try:
    test_resp = urllib.request.urlopen(test_req, timeout=60)
    result = json.loads(test_resp.read().decode())
    print(json.dumps(result, indent=2))
    
    print("\n=== RESULTS ===")
    if result.get("email_sent"):
        print(f"✅ EMAIL: Sent to {result.get('email_recipients', [])}")
    else:
        print(f"❌ EMAIL: {result.get('email_error', 'Not configured')}")
    
    if result.get("sms_sent"):
        print(f"✅ SMS: Sent to {result.get('sms_target', '?')}")
    else:
        print(f"❌ SMS: {result.get('sms_error', 'Not configured')}")

except urllib.error.HTTPError as e:
    body = e.read().decode()[:500]
    print(f"Test notification failed: HTTP {e.code}")
    print(body)
except Exception as e:
    print(f"Error: {e}")
