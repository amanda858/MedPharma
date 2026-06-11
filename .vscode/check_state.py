import json, urllib.request, sys
# Login
data = json.dumps({"username":"admin","password":"admin123"}).encode()
req = urllib.request.Request(
    "https://medpharma-hub.onrender.com/hub/api/login",
    data=data, headers={"Content-Type":"application/json"})
resp = urllib.request.urlopen(req, timeout=30)
cookie = ""
for ck in (resp.headers.get("Set-Cookie","")).split(", "):
    if ck.startswith("hub_session="):
        cookie = ck.split(";",1)[0]
# Fetch clients
req = urllib.request.Request(
    "https://medpharma-hub.onrender.com/hub/api/clients",
    headers={"Cookie": cookie})
d = json.loads(urllib.request.urlopen(req, timeout=30).read())
clients = d if isinstance(d, list) else d.get("clients", [])
print(f"Total accounts: {len(clients)}")
for c in clients:
    print(f"  [{c.get('role','?'):6}] {c.get('username','?'):35} {c.get('company','')}")
