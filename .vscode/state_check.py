import subprocess, urllib.request
r = subprocess.run(["git","log","--oneline","-15"], cwd="/workspaces/CVOPro", capture_output=True, text=True)
print("=== git log ===")
print(r.stdout)
r = subprocess.run(["git","show","--stat","HEAD"], cwd="/workspaces/CVOPro", capture_output=True, text=True)
print("=== HEAD show ===")
print(r.stdout[:3000])
print("=== healthz ===")
try:
    with urllib.request.urlopen("https://medpharma-hub.onrender.com/healthz", timeout=30) as resp:
        print("HTTP", resp.status, resp.read().decode()[:300])
except Exception as e:
    print("ERR", e)
