"""Commit the staged susan/melissa auth fix and push to origin/main.

Only commits the two files we care about so we don't sweep up unrelated
working-tree changes.
"""
from __future__ import annotations

import subprocess
import sys

REPO = "/workspaces/CVOPro"
FILES = [
    "app/client_db.py",
    "app/client_routes.py",
    "app/templates/client_hub.html",
    "data/clients_seed.json",
]
MESSAGE = "ui(login): clean centered card, remove brand wordmark + secure-sign-in badge"


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"\n$ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    if r.stdout:
        print(r.stdout.rstrip())
    if r.stderr:
        print(r.stderr.rstrip(), file=sys.stderr)
    if check and r.returncode != 0:
        sys.exit(r.returncode)
    return r


def main() -> int:
    # 1) Force-stage the two files (data/ is in .gitignore, but the file is
    #    already tracked — -f is needed because the directory is ignored).
    run(["git", "add", "-f", "--", *FILES])

    # 2) Show what's about to be committed
    run(["git", "diff", "--cached", "--stat", "--", *FILES])

    # 3) Commit. If nothing staged for those files, skip but keep going.
    cached = run(["git", "diff", "--cached", "--name-only", "--", *FILES], check=False)
    if not cached.stdout.strip():
        print("\nNo staged changes for susan/melissa files — skipping commit.")
    else:
        run(["git", "commit", "-m", MESSAGE])

    # 4) Push to origin/main
    run(["git", "push", "origin", "main"])

    # 5) Confirm
    run(["git", "log", "--oneline", "-3"])
    print("\n=== DONE — wait ~2 min for Render to redeploy ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
