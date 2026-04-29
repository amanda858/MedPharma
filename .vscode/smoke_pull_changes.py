#!/usr/bin/env python3
"""Smoke test: import app.national_pull and app.leads_app, confirm signature."""
import sys, os, inspect
sys.path.insert(0, "/workspaces/CVOPro")
os.environ.setdefault("DB_PATH", "/tmp/leads_smoke.db")
os.environ.setdefault("NATIONAL_PULL_DIR", "/tmp/national_pulls_smoke")

from app import national_pull
print("national_pull import OK")
sig = inspect.signature(national_pull._run_pull_async)
print(f"_run_pull_async signature: {sig}")
assert "states" in sig.parameters
assert "per_state" in sig.parameters
assert "specialty" in sig.parameters
print("signature has states, per_state, specialty: OK")

from app import leads_app
print("leads_app import OK")

# verify the endpoint accepts params
routes = [r for r in leads_app.app.routes if getattr(r, "path", "") == "/api/national-pull/run"]
assert routes, "run endpoint not registered"
print(f"run endpoint registered: methods={routes[0].methods}")
print("ALL OK")
