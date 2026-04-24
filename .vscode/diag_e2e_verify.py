"""End-to-end test: scrub a CSV via live API, then verify the output locally."""
import urllib.request, urllib.parse, http.cookiejar, json, time, os, sys, asyncio
sys.path.insert(0, "/workspaces/CVOPro")

base = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
req = urllib.request.Request(base+"/hub/api/login", data=json.dumps({"username":"admin","password":"admin123"}).encode(), headers={"Content-Type":"application/json"})
op.open(req, timeout=60).read()

# Real-ish test set: a mix of orgs we expect to have working email vs not
csv_input = (
    b"organization_name,city,state,website\n"
    b"Quest Diagnostics,Secaucus,NJ,questdiagnostics.com\n"
    b"LabCorp,Burlington,NC,labcorp.com\n"
    b"Mayo Clinic Laboratories,Rochester,MN,mayocliniclabs.com\n"
    b"GitHub Inc,San Francisco,CA,github.com\n"
)
boundary = "----X"
body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"t.csv\"\r\nContent-Type: text/csv\r\n\r\n").encode() + csv_input + f"\r\n--{boundary}--\r\n".encode()
r = op.open(urllib.request.Request(base+"/admin/leads/api/scrub/upload?max_rows=10", data=body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}), timeout=60)
jid = json.loads(r.read())["job_id"]
print(f"[scrub] job_id={jid}")

for i in range(40):
    time.sleep(4)
    sj = json.loads(op.open(base+f"/admin/leads/api/scrub/status/{jid}", timeout=30).read())
    if sj.get("status") == "done":
        break
    print(f"  [{i}] {sj.get('status')} {sj.get('done_rows')}/{sj.get('total_rows')}")
print(f"[scrub] done — {sj['summary']['rows_with_email']}/{sj['summary']['input_rows']} rows had emails")

scrubbed = op.open(base+f"/admin/leads/api/scrub/download/{jid}.csv", timeout=60).read().decode()
in_path = "/tmp/scrubbed_e2e.csv"
out_path = "/tmp/scrubbed_e2e_verified.csv"
with open(in_path, "w") as f:
    f.write(scrubbed)
print(f"[scrub] wrote {in_path}")

# Now verify locally (port 25 works in dev container)
from verify_csv import main as verify_main
asyncio.run(verify_main(in_path, out_path))

# Print final results
print("\n=== FINAL VERIFIED CSV ===")
import csv as _csv
with open(out_path) as f:
    rdr = _csv.DictReader(f)
    for row in rdr:
        print(f"\n{row['Org Name']} ({row.get('Verified Domain') or '—'})")
        for i in range(1, 6):
            e = row.get(f"Email {i}", "")
            if not e:
                continue
            v = row.get(f"Email {i} Verdict", "")
            sc = row.get(f"Email {i} Score", "")
            mx = row.get(f"Email {i} MX", "")
            sm = row.get(f"Email {i} SMTP", "")
            print(f"  {i}. {e:50s}  verdict={v:14s} score={sc} mx={mx} smtp={sm}")
