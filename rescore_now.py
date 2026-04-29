#!/usr/bin/env python3
"""Re-score using cached data (skip fetch). Then rebuild FINAL CSVs."""
import csv, os, subprocess, sys
ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "output")

print("=== RUN rule_intercept (rescore cached data) ===")
r = subprocess.run([sys.executable, "rule_intercept.py"], cwd=ROOT)
if r.returncode != 0:
    sys.exit(r.returncode)

src = os.path.join(OUT, "labs_routed_full.csv")
apollo_path = os.path.join(OUT, "FINAL_apollo_upload.csv")
top_path = os.path.join(OUT, "FINAL_top_50.csv")

apollo_fields = ["Company Name","City","State","Zip","NPI","CLIA","Tier",
                 "Rule Score","Engine Score","Priority","Lab Type","Signals","Target Roles"]
target_roles = "Lab Director; Billing Manager; RCM Director; Compliance Officer; COO; CEO; Credentialing Specialist"

rows = list(csv.DictReader(open(src, encoding="utf-8")))
seen = set()
out_rows = []
for r in rows:
    key = (r.get("org_name","").lower(), r.get("city","").lower(), r.get("state","").lower())
    if key in seen or not r.get("org_name"):
        continue
    seen.add(key)
    out_rows.append({
        "Company Name": r.get("org_name",""),
        "City": r.get("city",""),
        "State": r.get("state",""),
        "Zip": r.get("zip",""),
        "NPI": r.get("npi",""),
        "CLIA": r.get("clia",""),
        "Tier": r.get("tier",""),
        "Rule Score": r.get("rule_score",""),
        "Engine Score": r.get("engine_score",""),
        "Priority": r.get("priority",""),
        "Lab Type": r.get("lab_type",""),
        "Signals": r.get("signals",""),
        "Target Roles": target_roles,
    })

with open(apollo_path,"w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=apollo_fields); w.writeheader(); w.writerows(out_rows)
with open(top_path,"w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=apollo_fields); w.writeheader(); w.writerows(out_rows[:50])

from collections import Counter
tc = Counter(r["Tier"] for r in out_rows)
print(f"\nTOTAL: {len(out_rows)}  TIERS: {dict(tc)}")
labcorp = sum(1 for r in out_rows if 'LABORATORY CORPORATION' in r['Company Name'].upper())
labcorp_a = sum(1 for r in out_rows if 'LABORATORY CORPORATION' in r['Company Name'].upper() and r['Tier']=='A')
print(f"LabCorp branches: {labcorp} (Tier A: {labcorp_a})")
ngs_only = sum(1 for r in out_rows if r['Tier']=='A' and 'ngs' in r['Signals'].lower() and not any(k in r['Company Name'].lower() for k in ['genomic','molecular','genetic','toxicol','dna','pgx','specialty','ngs ',' ngs','ngs,']))
print(f"Tier A with 'ngs' signal but no genuine ngs/molecular keyword: {ngs_only}")
