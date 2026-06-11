"""Wait for Render to deploy commit 136d3a2 then re-run the scoping check."""
import time, json, urllib.request, sys

TARGET = "136d3a2"

def check_build():
    try:
        req = urllib.request.Request("https://medpharma-hub.onrender.com/hub/api/build-info", headers={})
        r = urllib.request.urlopen(req, timeout=10)
        info = json.loads(r.read())
        return info.get("commit","")[:7]
    except Exception as e:
        return f"err:{e}"

print(f"Waiting for commit {TARGET[:7]} on live...")
start = time.time()
for i in range(40):
    cur = check_build()
    elapsed = int(time.time() - start)
    print(f"  [{elapsed:3d}s] live commit = {cur}")
    if cur == TARGET[:7]:
        print("  DEPLOYED")
        sys.exit(0)
    time.sleep(15)
print("  TIMEOUT")
sys.exit(1)
