"""Audit client_hub.html across recent commits to confirm chat UI never made
it into git. If it did, we can recover it.
"""
import subprocess

ROOT = "/workspaces/MedPharma"


def run(cmd, max_chars=3000):
    print(f"\n$ {' '.join(cmd)}")
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    s = out.stdout or ""
    if len(s) > max_chars:
        print(s[:max_chars])
        print(f"... [truncated {len(s) - max_chars} chars]")
    else:
        print(s)
    if out.stderr:
        print("STDERR:", out.stderr[:600])


# Last 20 commits touching the file
run(["git", "log", "--oneline", "-20", "--", "app/templates/client_hub.html"])

# For each of the last 5 commits, grep for chat / moduleOpt / userAccess markers
for ref in ["HEAD", "HEAD~1", "HEAD~2", "HEAD~3", "HEAD~4",
            "2ecc17e", "d0ff77b", "a9f0bdf"]:
    print(f"\n--- {ref}: app/templates/client_hub.html chat/opt markers ---")
    out = subprocess.run(
        ["git", "show", f"{ref}:app/templates/client_hub.html"],
        cwd=ROOT, capture_output=True, text=True,
    )
    if out.returncode != 0:
        print(f"  (not present in {ref})")
        continue
    html = out.stdout
    markers = [
        "panel-chat", "chatRoomModal", "chatFab", "chatMembersModal",
        "moduleOptCard", "data-module=", "applyModuleVisibility",
        "userAccess", "accessUserPicker", "accountAccess",
        "client_user_access", "Add Client", "client-dashboard",
    ]
    found = [m for m in markers if m in html]
    print(f"  size={len(html)} chars, markers found: {found or 'NONE'}")
