"""Snapshot of uncommitted diffs in active app code, so we can decide what's
real work vs garbage. Prints sizes so we don't blow up the terminal.
"""
import subprocess

ROOT = "/workspaces/CVOPro"


def run(cmd, max_chars=4000):
    print(f"\n$ {' '.join(cmd)}")
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if out.stdout:
        text = out.stdout
        if len(text) > max_chars:
            print(text[:max_chars])
            print(f"... [truncated {len(text) - max_chars} chars]")
        else:
            print(text)
    if out.stderr:
        print("STDERR:", out.stderr[:500])


# Per-file stats first
run(["git", "diff", "--stat", "HEAD"], max_chars=4000)
print("\n\n--- Per-file diffs (capped) ---")
for path in [
    "app/client_db.py",
    "app/client_routes.py",
    "app/notifications.py",
    "app/scrubber.py",
    "app/hub_app.py",
    "run.py",
    "run_local.py",
    "render.yaml",
    "README.md",
    "test_imports.py",
    ".vscode/auth_smoke.py",
    ".vscode/full_admin_smoke.py",
]:
    print(f"\n========= {path} =========")
    run(["git", "diff", "HEAD", "--", path], max_chars=2500)
