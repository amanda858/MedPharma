#!/usr/bin/env python3
"""
import_to_hub.py — push routed leads from output/labs_routed_full.csv
straight into the hub's saved_leads database. No CSV upload required.

After this runs, the leads appear in your hub UI immediately.
"""
import csv
import os
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from app.database import init_db, save_lead, get_db, _run_write_with_retry  # noqa: E402

SRC = os.path.join(ROOT, "output", "labs_routed_full.csv")
if not os.path.exists(SRC):
    print(f"!! missing {SRC} — run `python3 go.py` first")
    sys.exit(1)

print(f"[INIT] {SRC}")
init_db()

rows = list(csv.DictReader(open(SRC, encoding="utf-8")))
print(f"[LOAD] {len(rows)} routed leads")

# Filter: skip Unknown tier, skip rows with no NPI
rows = [r for r in rows if r.get("npi") and r.get("tier") in ("A", "B", "C")]
print(f"[FILTER] {len(rows)} have NPI + valid tier")

# Sort: best leads first
TIER_RANK = {"A": 0, "B": 1, "C": 2}
rows.sort(key=lambda r: (TIER_RANK.get(r.get("tier"), 9), -int(r.get("rule_score") or 0)))

inserted = 0
skipped = 0
for r in rows:
    try:
        score = int(r.get("rule_score") or 0)
    except ValueError:
        score = 0
    tier = r.get("tier", "")
    tags = f"tier-{tier};lab;rule-intercept"
    notes = (
        f"Tier {tier} | RuleScore {score} | "
        f"Lab Type: {r.get('lab_type','')} | "
        f"Signals: {r.get('signals','')}"
    )
    lead = {
        "npi": r.get("npi"),
        "organization_name": r.get("org_name", ""),
        "first_name": "",
        "last_name": "",
        "credential": "",
        "taxonomy_code": "",
        "taxonomy_desc": r.get("lab_type", ""),
        "address_line1": "",
        "address_line2": "",
        "city": r.get("city", ""),
        "state": r.get("state", ""),
        "zip_code": r.get("zip", ""),
        "phone": "",
        "fax": "",
        "enumeration_date": "",
        "last_updated": datetime.now().isoformat(),
        "lead_score": score,
        "lead_status": "new",
        "notes": notes,
        "tags": tags,
        "source": "rule-intercept",
    }
    try:
        save_lead(lead)
        inserted += 1
    except Exception as exc:
        skipped += 1
        if skipped <= 3:
            print(f"  skip {r.get('npi')}: {exc}")

# Verify
conn = get_db()
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM saved_leads")
total_db = cur.fetchone()[0]
cur.execute("SELECT state, COUNT(*) FROM saved_leads GROUP BY state ORDER BY 2 DESC LIMIT 10")
top_states = cur.fetchall()
cur.execute("SELECT source, COUNT(*) FROM saved_leads GROUP BY source")
by_src = cur.fetchall()
conn.close()

print()
print("========== HUB IMPORT DONE ==========")
print(f"Inserted/updated : {inserted}")
print(f"Skipped          : {skipped}")
print(f"Total in DB      : {total_db}")
print(f"By source        : {[dict(r) if hasattr(r,'keys') else tuple(r) for r in by_src]}")
print(f"Top states in DB : {[tuple(r) for r in top_states]}")
print()
print("Open the hub — leads are live in saved_leads now.")
