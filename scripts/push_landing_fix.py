"""Step 1: push ONLY the landing-page revert to production.

This stages exactly one file (app/templates/client_hub.html), commits with a
clear message, and pushes to origin/main so Render redeploys the original UI.

In-progress feature work in app/client_db.py, app/client_routes.py, etc. is
intentionally NOT included.
"""
import subprocess
import sys

ROOT = "/workspaces/MedPharma"


def run(cmd, check=True):
    print(f"\n$ {' '.join(cmd)}")
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if out.stdout:
        print(out.stdout)
    if out.stderr:
        print("STDERR:", out.stderr)
    if check and out.returncode != 0:
        sys.exit(f"Command failed: {cmd}")
    return out


# Confirm exactly which files will be staged
run(["git", "status", "--short", "--", "app/templates/client_hub.html"])

# Stage only the landing-page file
run(["git", "add", "--", "app/templates/client_hub.html"])

# Show staged change summary (sanity check)
run(["git", "diff", "--cached", "--stat"])

# Commit
run([
    "git", "commit", "-m",
    "ui(login): revert to premium split-screen design (rollback 2ecc17e)",
    "-m",
    "Restores the hero-panel login UI that operators rely on. The 2ecc17e "
    "'clean centered card' rewrite was rolled back because it removed brand "
    "elements and looked unfinished in production.",
])

# Push
run(["git", "push", "origin", "main"])

# Show last commit + remote tip
run(["git", "log", "--oneline", "-3"])
print("\n✅ Landing page revert pushed. Render will redeploy in ~1-2 min.")
