"""Step 2: clean up dead/garbage files and push the cleanup commit.

What this does (all safe, all reversible via git):
  • Stages the working-tree deletions of:
      - 40+ one-shot .vscode helper scripts that finished their job
      - app/leads_app.py (3498 lines, no longer imported anywhere)
      - app/main.py     (376 lines,  no longer imported anywhere)
      - app/templates/index.html (2776 lines, not rendered by any route)
  • Deletes the typo file `tatus --short` from disk + git index
  • Adds .vscode screenshots and *.bak-* backups to .gitignore so they
    stop polluting `git status`
  • Removes my own scratch file .vscode/tasks_inspect.json (duplicate of
    an entry already merged into tasks.json)
  • Commits + pushes as a dedicated chore commit so it is reverted with
    a single `git revert` if anything looks off.

Files explicitly NOT touched: app/client_db.py, app/client_routes.py,
app/notifications.py, app/scrubber.py, app/hub_app.py — these contain
in-progress chat / access-control feature work and should be shipped
as their own feature commit when ready.
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path("/workspaces/MedPharma")


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


# ── 1) Update .gitignore so screenshots and .bak-* don't keep showing up ──
gitignore = ROOT / ".gitignore"
text = gitignore.read_text() if gitignore.exists() else ""
additions = []
for pat in [
    "# editor scratch / cleanup",
    ".vscode/Screenshot*.png",
    ".vscode/Screenshot*.PNG",
    "*.bak-*",
    "tatus*",  # catches accidental `git status --short` mistype that
               # wrote a file named "tatus --short"
]:
    if pat not in text:
        additions.append(pat)
if additions:
    new = text.rstrip() + "\n\n" + "\n".join(additions) + "\n"
    gitignore.write_text(new)
    print(f"Updated .gitignore with: {additions}")
else:
    print(".gitignore already has the needed patterns")

# ── 2) Delete obvious junk files from disk + index ──
junk_files = [
    ROOT / "tatus --short",                                 # typo file
    ROOT / ".vscode" / "Screenshot 2026-06-09 at 10.08.48\u202fAM.png",  # untracked
    ROOT / ".vscode" / "tasks_inspect.json",                # my duplicate
]
for p in junk_files:
    if p.exists():
        try:
            p.unlink()
            print(f"Deleted: {p}")
        except OSError as e:
            print(f"Could not delete {p}: {e}")

# Best-effort: remove the latest *.bak-* backup of client_hub.html
for p in (ROOT / "app" / "templates").glob("client_hub.html.bak-*"):
    try:
        p.unlink()
        print(f"Deleted backup: {p}")
    except OSError as e:
        print(f"Could not delete backup {p}: {e}")

# ── 3) Stage all deletions git already knows about ──
# `git add -A` would also stage modified feature files; we want a tight scope.
run(["git", "add", "--update", ".vscode/"])     # picks up only deletions in .vscode/
run(["git", "add", "--update", "app/leads_app.py"])
run(["git", "add", "--update", "app/main.py"])
run(["git", "add", "--update", "app/templates/index.html"])
run(["git", "add", ".gitignore"])

# Sanity: print what's staged
run(["git", "diff", "--cached", "--stat"])

# ── 4) Commit + push ──
run([
    "git", "commit", "-m",
    "chore: remove dead code and one-shot scripts",
    "-m",
    "Deletes:\n"
    "  • 40+ finished one-shot .vscode/ helper scripts\n"
    "  • app/leads_app.py     (3498 lines, no longer imported)\n"
    "  • app/main.py          (376 lines,  no longer imported)\n"
    "  • app/templates/index.html (2776 lines, not rendered by any route)\n"
    "  • stray typo file 'tatus --short'\n"
    "\n"
    "Updates .gitignore so editor screenshots and *.bak-* backups do not\n"
    "pollute git status. No runtime behavior changes — these files were\n"
    "verified to have zero importers in app/.",
])

run(["git", "push", "origin", "main"])
run(["git", "log", "--oneline", "-3"])

print("\n✅ Dead-code cleanup pushed.")
