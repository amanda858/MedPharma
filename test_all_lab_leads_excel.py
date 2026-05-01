"""
test_all_lab_leads_excel.py

Validates the 'All Lab Leads.xlsx' file through the upgraded lab intelligence pipeline:
  1. Parses the Excel file
  2. Detects column mapping
  3. Runs score_lab_lead (rule_intercept) on every row
  4. Shows tier breakdown + top leads
"""

import os
import sys

EXCEL_PATH = os.path.join(os.path.dirname(__file__), ".devcontainer", "All Lab Leads.xlsx")

if not os.path.exists(EXCEL_PATH):
    print(f"❌  File not found: {EXCEL_PATH}")
    sys.exit(1)

file_size = os.path.getsize(EXCEL_PATH)
print(f"✅  Found: {EXCEL_PATH}  ({file_size:,} bytes)")

from app.scrubber import parse_uploaded, detect_columns
from rule_intercept import score_lab_lead

with open(EXCEL_PATH, "rb") as fh:
    content = fh.read()

headers, rows = parse_uploaded(content, "All Lab Leads.xlsx")

print(f"\n📊  {len(headers)} columns  |  {len(rows)} data rows")
print(f"    Headers: {headers}")

col_map = detect_columns(headers)
name_col  = col_map.get("name")
state_col = col_map.get("state")
spec_col  = col_map.get("taxonomy")

print(f"\n🗺️   Mapping  →  name='{name_col}'  state='{state_col}'  type='{spec_col}'\n")

# Score every row
results = []
for row in rows:
    org   = row.get(name_col, "") if name_col else ""
    state = row.get(state_col, "") if state_col else ""
    typ   = row.get(spec_col, "")  if spec_col  else ""
    intel = score_lab_lead(org, lab_type=typ, state=state)
    results.append((intel, org, state, typ))

# Sort by score descending
results.sort(key=lambda x: -x[0]["score"])

# Tier breakdown
tiers = {"A": 0, "B": 0, "C": 0, "Unknown": 0}
for intel, *_ in results:
    tiers[intel["tier"]] = tiers.get(intel["tier"], 0) + 1

print("📈  Tier breakdown:")
for tier, count in sorted(tiers.items()):
    label = {"A": "Tier A — specialty/tox/molecular (High)",
             "B": "Tier B — clinical pathology/hematology (Medium)",
             "C": "Tier C — general/commodity (Low)",
             "Unknown": "Unknown — no lab type matched"}.get(tier, tier)
    pct = count / len(results) * 100
    bar = "█" * int(pct / 4)
    print(f"    {tier}  {count:>4}  ({pct:>5.1f}%)  {bar}  {label}")

# Top 20 leads
print(f"\n🏆  Top 20 leads by score:")
print(f"    {'Score':<7} {'Tier':<6} {'Priority':<9} {'Lab Type':<22} {'State':<7} {'Lab Name'}")
print(f"    {'─'*7} {'─'*6} {'─'*9} {'─'*22} {'─'*7} {'─'*40}")
for intel, org, state, typ in results[:20]:
    detected = intel.get("lab_type_detected", "") or typ[:20]
    print(f"    {intel['score']:<7} {intel['tier']:<6} {intel['priority']:<9} {detected[:22]:<22} {state:<7} {org[:45]}")

# Score distribution
score_buckets = {"80-100": 0, "60-79": 0, "40-59": 0, "20-39": 0, "0-19": 0}
for intel, *_ in results:
    s = intel["score"]
    if s >= 80:   score_buckets["80-100"] += 1
    elif s >= 60: score_buckets["60-79"] += 1
    elif s >= 40: score_buckets["40-59"] += 1
    elif s >= 20: score_buckets["20-39"] += 1
    else:         score_buckets["0-19"]  += 1

print(f"\n📊  Score distribution:")
for bucket, count in score_buckets.items():
    bar = "█" * (count // 3)
    print(f"    {bucket:<8}  {count:>4}  {bar}")

print(f"\n✅  {len(results)} leads scored  |  "
      f"Tier A: {tiers['A']}  Tier B: {tiers['B']}  Tier C: {tiers['C']}  Unknown: {tiers.get('Unknown',0)}")
print(f"    Scrub CSV/Excel tab will now output these with Lead Score, Lab Tier, Direct Line columns.")
