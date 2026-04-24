import urllib.request, urllib.parse, http.cookiejar, time, json, sys
base = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
try:
    req = urllib.request.Request(base+"/hub/api/login", data=json.dumps({"username":"admin","password":"admin123"}).encode(), headers={"Content-Type":"application/json"})
    op.open(req, timeout=60).read()
except Exception as e:
    print("login err:", e); sys.exit(1)
print("cookies:", [c.name for c in cj])

csv = b"organization_name,city,state,website\nQuest Diagnostics,Secaucus,NJ,questdiagnostics.com\nLabCorp,Burlington,NC,labcorp.com\nMayo Clinic Laboratories,Rochester,MN,mayocliniclabs.com\n"
boundary = "----DiagBoundary"
body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"t.csv\"\r\nContent-Type: text/csv\r\n\r\n").encode() + csv + f"\r\n--{boundary}--\r\n".encode()
req = urllib.request.Request(base+"/admin/leads/api/scrub/upload?max_rows=5", data=body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
try:
    r = op.open(req, timeout=120)
    txt = r.read().decode()
    print("upload status:", r.status, txt[:400])
    j = json.loads(txt)
    jid = j.get("job_id")
except urllib.error.HTTPError as e:
    print("HTTP", e.code, e.read()[:400]); sys.exit(2)

for i in range(80):
    time.sleep(4)
    try:
        s = op.open(base+f"/admin/leads/api/scrub/status/{jid}", timeout=30).read().decode()
        sj = json.loads(s)
        print(f"[{i}] status={sj.get('status')} done={sj.get('done_rows')}/{sj.get('total_rows')} err={sj.get('error')}")
        if sj.get("status") in ("done","error"):
            break
    except Exception as e:
        print("status err:", e)

if sj.get("status") == "done":
    print("summary:", json.dumps(sj.get("summary"), indent=2)[:1500])
    out = op.open(base+f"/admin/leads/api/scrub/download/{jid}.csv", timeout=60).read().decode()
    print("--- OUTPUT CSV ---")
    print(out[:3000])
