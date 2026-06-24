"""Backup current client_hub.html, then restore it to the version BEFORE
the 2ecc17e 'ui(login): clean centered card...' commit.

Run with: python3 scripts/restore_landing_inspect.py
"""
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path("/workspaces/MedPharma")
TARGET = ROOT / "app" / "templates" / "client_hub.html"
BAD_COMMIT = "2ecc17e"   # the commit that introduced the new login UI
RESTORE_REF = f"{BAD_COMMIT}^"  # the parent = state just before the bad UI


def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if out.stdout:
        print(out.stdout[:4000])
    if out.stderr:
        print("STDERR:", out.stderr[:2000])
    return out


# 1) Backup the current (possibly modified) file so the change is reversible.
stamp = time.strftime("%Y%m%d-%H%M%S")
backup = TARGET.with_suffix(f".html.bak-{stamp}")
shutil.copy2(TARGET, backup)
print(f"Backed up current landing page to: {backup}")

# 2) Restore from git
run(["git", "checkout", RESTORE_REF, "--", "app/templates/client_hub.html"])

# 3) Show what changed vs the bad version
run(["git", "diff", "--stat", "HEAD", "--", "app/templates/client_hub.html"])
print("\nRestore complete. Reload the landing page (hard refresh in browser).")
