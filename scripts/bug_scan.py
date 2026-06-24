"""Bug scan + import smoke test for the live hub code paths.

Reads source files directly (no imports) to find likely-broken patterns,
THEN does a real `import app.hub_app` to surface any breakage from the
deletions we just shipped.
"""
import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path("/workspaces/MedPharma")

# Live code paths only — the modules that ship to Render.
LIVE_FILES = [
    "app/__init__.py",
    "app/hub_app.py",
    "app/client_db.py",
    "app/client_routes.py",
    "app/notifications.py",
    "app/scrubber.py",
    "app/config.py",
    "app/database.py",
    "app/build_info.py",
    "run.py",
]


def scan(path: Path):
    """Return list of (line_no, kind, snippet) findings."""
    findings = []
    try:
        src = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return [(0, "IOERR", str(e))]
    lines = src.splitlines()

    # 1) Syntax check (catches things the editor missed)
    try:
        ast.parse(src, filename=str(path))
    except SyntaxError as e:
        findings.append((e.lineno or 0, "SYNTAX", f"{e.msg}: {e.text!r}"))

    for i, line in enumerate(lines, start=1):
        stripped = line.strip()

        # 2) bare excepts
        if re.match(r"^except\s*:", stripped):
            findings.append((i, "BARE-EXCEPT", line.rstrip()))

        # 3) SQL string-formatted execute() — only flag when % or .format
        #    inline with execute(. Real parameterised queries use ? or :name.
        if re.search(r"\.execute\(\s*[fF]?[\"']", line) and (
            "%s" in line or "{}" in line or "{0" in line or ".format(" in line
        ):
            findings.append((i, "SQL-FORMAT", line.rstrip()))

        # 4) f-string in execute() — same risk
        if re.search(r"\.execute\(\s*f[\"']", line):
            findings.append((i, "SQL-FSTRING", line.rstrip()))

        # 5) Mutable default args
        m = re.search(r"def\s+\w+\([^)]*=\s*(\[\]|\{\})", line)
        if m:
            findings.append((i, "MUT-DEFAULT", line.rstrip()))

        # 6) print() in production code (we use the logger)
        if path.parts[-2:] == ("app",) or "app/" in path.as_posix():
            if re.match(r"^\s*print\(", line) and "log." not in line:
                # Only flag in app/ (run.py + tests are OK)
                if path.as_posix().startswith("app/"):
                    findings.append((i, "PRINT-IN-APP", line.rstrip()))

    return findings


def main():
    print("=" * 70)
    print("BUG SCAN — live hub code paths")
    print("=" * 70)

    total = 0
    for rel in LIVE_FILES:
        p = ROOT / rel
        if not p.exists():
            print(f"\n❌ MISSING: {rel}")
            total += 1
            continue
        results = scan(p)
        if not results:
            print(f"\n✅ {rel}: clean")
            continue
        print(f"\n⚠️  {rel}: {len(results)} finding(s)")
        for ln, kind, snip in results[:25]:
            print(f"   L{ln:>4}  {kind:<12}  {snip[:120]}")
        if len(results) > 25:
            print(f"   ... ({len(results) - 25} more)")
        total += len(results)

    print(f"\nTotal findings: {total}")

    # ── Real import smoke test (catches anything we broke by deleting files) ──
    print("\n" + "=" * 70)
    print("IMPORT SMOKE TEST")
    print("=" * 70)
    test = (
        "import importlib, sys; "
        "mods = ['app.hub_app', 'app.client_db', 'app.client_routes', "
        "'app.notifications', 'app.scrubber', 'app.config']; "
        "[importlib.import_module(m) for m in mods]; "
        "print('all imports OK:', mods)"
    )
    out = subprocess.run(
        [sys.executable, "-c", test],
        cwd=ROOT, capture_output=True, text=True,
    )
    if out.stdout:
        print(out.stdout)
    if out.stderr:
        print("STDERR:", out.stderr)
    if out.returncode != 0:
        print("❌ Import smoke test FAILED")
        sys.exit(1)
    print("\n✅ All live modules import cleanly after cleanup.")


if __name__ == "__main__":
    main()
