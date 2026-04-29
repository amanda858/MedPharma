#!/usr/bin/env python3
"""
go.py — single entry point. Runs the whole pipeline end-to-end
and writes the final Apollo upload CSV.

    python3 go.py
"""
import csv
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "output")
os.makedirs(OUT, exist_ok=True)


def step(name, cmd):
    print(f"\n=== {name} ===")
    r = subprocess.run(cmd, cwd=ROOT)
    if r.returncode != 0:
        print(f"!! {name} failed (exit {r.returncode})")
        sys.exit(r.returncode)


# 1. Always do a fresh broad fetch (all 50 states, ~600 orgs each)
step("FETCH public data", [sys.executable, "fetch_public_data.py",
                           "--max-per-state", "600", "--skip-clia"])

# 2. Run rule intercept + lab engine -> output/labs_routed_full.csv
step("RUN rule_intercept", [sys.executable, "rule_intercept.py"])

# 3. Build Apollo CSV directly from labs_routed_full.csv (skip the seed-only path)
src = os.path.join(OUT, "labs_routed_full.csv")
if not os.path.exists(src):
    print(f"!! {src} missing")
    sys.exit(1)

apollo_path = os.path.join(OUT, "FINAL_apollo_upload.csv")
top_path = os.path.join(OUT, "FINAL_top_50.csv")

rows = []
with open(src, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        rows.append(r)

# already tier-sorted by rule_intercept
apollo_fields = ["Company Name", "City", "State", "Zip", "NPI", "CLIA", "Tier",
                 "Rule Score", "Engine Score", "Priority", "Lab Type",
                 "Signals", "Target Roles"]
target_roles = "Lab Director; Billing Manager; RCM Director; Compliance Officer; COO; CEO; Credentialing Specialist"

seen = set()
out_rows = []
for r in rows:
    key = (r.get("org_name", "").lower(), r.get("city", "").lower(), r.get("state", "").lower())
    if key in seen or not r.get("org_name"):
        continue
    seen.add(key)
    out_rows.append({
        "Company Name": r.get("org_name", ""),
        "City": r.get("city", ""),
        "State": r.get("state", ""),
        "Zip": r.get("zip", ""),
        "NPI": r.get("npi", ""),
        "CLIA": r.get("clia", ""),
        "Tier": r.get("tier", ""),
        "Rule Score": r.get("rule_score", ""),
        "Engine Score": r.get("engine_score", ""),
        "Priority": r.get("priority", ""),
        "Lab Type": r.get("lab_type", ""),
        "Signals": r.get("signals", ""),
        "Target Roles": target_roles,
    })

with open(apollo_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=apollo_fields)
    w.writeheader()
    w.writerows(out_rows)

with open(top_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=apollo_fields)
    w.writeheader()
    w.writerows(out_rows[:50])

# Summary
tier_counts = {"A": 0, "B": 0, "C": 0, "Unknown": 0}
for r in out_rows:
    tier_counts[r["Tier"]] = tier_counts.get(r["Tier"], 0) + 1

print("\n========== DONE ==========")
print(f"Total companies : {len(out_rows)}")
print(f"  Tier A        : {tier_counts.get('A', 0)}")
print(f"  Tier B        : {tier_counts.get('B', 0)}")
print(f"  Tier C        : {tier_counts.get('C', 0)}")
print(f"  Unknown       : {tier_counts.get('Unknown', 0)}")
print(f"\nFinal Apollo upload : {apollo_path}")
print(f"Top 50 outbound     : {top_path}")
print("\nUpload FINAL_apollo_upload.csv to Apollo. That's it.")
