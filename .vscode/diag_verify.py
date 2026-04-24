import urllib.request, urllib.parse, http.cookiejar, json
base = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
req = urllib.request.Request(base+"/hub/api/login", data=json.dumps({"username":"admin","password":"admin123"}).encode(), headers={"Content-Type":"application/json"})
op.open(req, timeout=60).read()

tests = [
    "ceo@google.com",                      # likely rejected (no real mailbox)
    "support@labcorp.com",                 # role at major lab
    "billing@questdiagnostics.com",        # role at major lab
    "info@mayocliniclabs.com",             # role at academic lab
    "noexist-zzzzz12345@google.com",       # definitely undeliverable
    "test@nonexistent-domain-xyz-123.com", # no MX
]

for t in tests:
    try:
        r = op.open(base+"/admin/leads/api/verify/email?addr="+urllib.parse.quote(t), timeout=30)
        d = json.loads(r.read())
        print(f"{t:50s} verdict={d.get('verdict'):14s} score={d.get('score'):3d} mx={d.get('mx_found')} smtp={d.get('smtp_result')} catchall={d.get('catchall')} reason={d.get('reason')}")
    except urllib.error.HTTPError as e:
        print(f"{t:50s} HTTP {e.code}: {e.read()[:200]}")
    except Exception as e:
        print(f"{t:50s} err: {e}")
