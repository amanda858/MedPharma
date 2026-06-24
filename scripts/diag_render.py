"""Diagnose 'render failed' — check the actual live service + recent GH Actions."""
import subprocess


def run(cmd, check=False):
    print(f"\n$ {' '.join(cmd)}")
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.stdout:
        print(out.stdout)
    if out.stderr:
        print("STDERR:", out.stderr)
    return out


# 1) Is the production hub actually responding?
run(["curl", "-sS", "-o", "/dev/null", "-w",
     "healthz: HTTP %{http_code} in %{time_total}s\\n",
     "--max-time", "30",
     "https://medpharma-hub.onrender.com/healthz"])

run(["curl", "-sS", "-o", "/dev/null", "-w",
     "readyz : HTTP %{http_code} in %{time_total}s\\n",
     "--max-time", "60",
     "https://medpharma-hub.onrender.com/readyz"])

run(["curl", "-sS", "-o", "/dev/null", "-w",
     "buildz : HTTP %{http_code} in %{time_total}s\\n",
     "--max-time", "60",
     "https://medpharma-hub.onrender.com/buildz"])

run(["curl", "-sS", "--max-time", "60",
     "https://medpharma-hub.onrender.com/buildz"])

# 2) Recent GitHub Actions runs
print("\n--- Recent GitHub Actions runs (last 10) ---")
run(["gh", "run", "list", "--limit", "10"])

# 3) Recent commits actually on origin/main
print("\n--- Recent commits on origin/main ---")
run(["git", "log", "--oneline", "-5", "origin/main"])
