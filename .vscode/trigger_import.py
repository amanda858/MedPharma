import urllib.request, json
BASE="https://medpharma-hub.onrender.com"
print("=== TRIGGER BUNDLED IMPORT ===")
r = urllib.request.Request(f"{BASE}/admin/leads/api/national-pull/import-bundled", method="POST", headers={"Content-Type":"application/json"})
try:
    with urllib.request.urlopen(r, timeout=300) as resp:
        body = resp.read().decode()
        print("HTTP", resp.status)
        print(body[:2000])
except Exception as e:
    print("ERR", e)
    try: print(e.read().decode()[:2000])
    except: pass
