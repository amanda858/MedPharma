#!/usr/bin/env python3
"""Score the existing seed_national_pull.csv with rule_intercept tiering."""
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rule_intercept import score_lab_lead

SRC = "app/seed/seed_national_pull.csv"
OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)
OUT = os.path.join(OUT_DIR, "seed_routed.csv")

rows_out = []
tiers = {"A": 0, "B": 0, "C": 0, "Unknown": 0}

with open(SRC, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        org = r.get("Org Name") or r.get("org_name") or ""
        ltype = r.get("Taxonomy / Type") or r.get("Type Detected") or ""
        st = r.get("State") or ""
        rule = score_lab_lead(org, ltype, st)
        tiers[rule["tier"]] = tiers.get(rule["tier"], 0) + 1
        rows_out.append({
            "Tier": rule["tier"],
            "Rule Score": rule["score"],
            "Priority": rule["priority"],
            "Org Name": org,
            "City": r.get("City", ""),
            "State": st,
            "Heat Score": r.get("Heat Score", ""),
            "Decision Maker": r.get("Decision Maker", ""),
            "DM Email": r.get("DM Email", ""),
            "Phone": r.get("Phone", ""),
            "Org Domain": r.get("Org Domain", ""),
            "Lab Type": ltype,
            "Signals": "; ".join(rule["signals"]),
        })

rows_out.sort(key=lambda x: (
    {"A": 0, "B": 1, "C": 2, "Unknown": 3}.get(x["Tier"], 9),
    -x["Rule Score"],
))

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
    w.writeheader()
    w.writerows(rows_out)

print(f"Source : {SRC}")
print(f"Output : {OUT}")
print(f"Total  : {len(rows_out)}")
print(f"Tiers  : A={tiers['A']}  B={tiers['B']}  C={tiers['C']}  Unknown={tiers['Unknown']}")
print()
print(f"{'Tier':<5} {'Score':<6} {'Org':<48} {'City':<18} {'ST':<3} {'DM Email'}")
print("-" * 130)
for r in rows_out:
    print(f"{r['Tier']:<5} {r['Rule Score']:<6} {r['Org Name'][:48]:<48} {r['City'][:18]:<18} {r['State']:<3} {r['DM Email']}")
