import subprocess, os, sys
os.chdir("/workspaces/CVOPro")
def sh(cmd):
    print(f"$ {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout)
    if r.stderr: print("STDERR:", r.stderr)
    return r.returncode
# Stage only what we need to ship
files = [
    "app/leads_app.py",
    "rule_intercept.py",
    "fetch_public_data.py",
    "go.py",
    "rescore_now.py",
    "import_to_hub.py",
    "output/labs_routed_full.csv",
    "output/labs_routed_top.csv",
    "output/labs_apollo_companies.csv",
    "output/FINAL_apollo_upload.csv",
    "output/FINAL_top_50.csv",
]
sh(["git","add","-f"] + files)
sh(["git","status","--short"])
rc = sh(["git","commit","-m","feat: live import endpoint + bundled routed leads CSV (13.4k tier A/B/C)"])
print("commit rc:", rc)
sh(["git","push","origin","main"])
sh(["git","log","--oneline","-3"])
