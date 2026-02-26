#!/usr/bin/env python3
"""Research script to check MedPharma Hub accounts and data."""
import subprocess
import json

# --- Part 1: Git diff ---
print("=" * 60)
print("GIT DIFF (last commit changes to client_routes.py and client_db.py)")
print("=" * 60)
try:
    result = subprocess.run(
        ["git", "diff", "HEAD~1", "--", "app/client_routes.py", "app/client_db.py"],
        capture_output=True, text=True, cwd="/workspaces/CVOPro"
    )
    lines = result.stdout.split("\n")[:300]
    print("\n".join(lines))
    if result.stderr:
        print("STDERR:", result.stderr[:500])
except Exception as e:
    print(f"Git diff error: {e}")

# --- Part 2: API checks ---
import urllib.request
import urllib.error
import http.cookiejar

BASE = "https://medpharma-hub.onrender.com"

accounts = [
    {"username": "admin", "password": "admin123"},
    {"username": "rcm", "password": "rcm123"},
    {"username": "jessica", "password": "jessica123"},
]

endpoints = ["/hub/api/claims", "/hub/api/credentialing", "/hub/api/files"]

for acct in accounts:
    print("\n" + "=" * 60)
    print(f"ACCOUNT: {acct['username']}")
    print("=" * 60)
    
    # Create cookie jar for session
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    
    # Login
    login_data = json.dumps(acct).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}/hub/api/login",
        data=login_data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    
    try:
        resp = opener.open(req, timeout=30)
        body = resp.read().decode("utf-8")
        login_result = json.loads(body)
        print(f"  Login: SUCCESS (HTTP {resp.status})")
        print(f"  Response: {json.dumps(login_result, indent=2)[:500]}")
        
        # Extract role/company
        if isinstance(login_result, dict):
            role = login_result.get("role", login_result.get("user", {}).get("role", "N/A") if isinstance(login_result.get("user"), dict) else "N/A")
            company = login_result.get("company", login_result.get("user", {}).get("company", "N/A") if isinstance(login_result.get("user"), dict) else "N/A")
            print(f"  Role: {role}")
            print(f"  Company: {company}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  Login: FAILED (HTTP {e.code})")
        print(f"  Error: {body[:500]}")
        continue
    except Exception as e:
        print(f"  Login: ERROR - {e}")
        continue
    
    # Check endpoints
    for ep in endpoints:
        print(f"\n  --- {ep} ---")
        req2 = urllib.request.Request(f"{BASE}{ep}", method="GET")
        try:
            resp2 = opener.open(req2, timeout=30)
            body2 = resp2.read().decode("utf-8")
            try:
                data = json.loads(body2)
                if isinstance(data, list):
                    print(f"  Count: {len(data)} records")
                    if data:
                        print(f"  First record keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'N/A'}")
                        print(f"  First record: {json.dumps(data[0], indent=2)[:400]}")
                elif isinstance(data, dict):
                    # Might be paginated or nested
                    for k, v in data.items():
                        if isinstance(v, list):
                            print(f"  '{k}': {len(v)} records")
                            if v and isinstance(v[0], dict):
                                print(f"    First record keys: {list(v[0].keys())}")
                                print(f"    First record: {json.dumps(v[0], indent=2)[:300]}")
                        else:
                            print(f"  '{k}': {v}")
                else:
                    print(f"  Data type: {type(data).__name__}")
                    print(f"  Content: {str(data)[:400]}")
            except json.JSONDecodeError:
                print(f"  Raw (not JSON): {body2[:400]}")
        except urllib.error.HTTPError as e:
            body2 = e.read().decode("utf-8", errors="replace")
            print(f"  HTTP {e.code}: {body2[:300]}")
        except Exception as e:
            print(f"  ERROR: {e}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
