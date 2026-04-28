#!/usr/bin/env python3
"""National daily lead pull — all 50 states, every day, no spot-checks.

Run once per day (cron, GitHub Action, Render cron, or manually). Output:
  /tmp/leads_national_<date>.csv  — full enriched leads, sorted by Heat Score
  /tmp/leads_top500_<date>.csv    — top 500 highest-heat leads to work today

Specialty defaults to clinical labs but can be overridden via SPECIALTY env.
Per-state cap defaults to 50 (50 × 50 = 2500 raw labs/day before dedup).
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, "/workspaces/CVOPro")

from app.bulk_prospector import prospect_multi_state, _enrich_dm_only

US_STATES_50 = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
]
# Plus DC + territories (NPPES has these)
US_STATES_PLUS = US_STATES_50 + ["DC", "PR"]

SPECIALTY = os.environ.get("SPECIALTY", "clinical")
PER_STATE = int(os.environ.get("PER_STATE", "50"))
NEW_ONLY = os.environ.get("NEW_ONLY", "0") == "1"
NEW_DAYS = int(os.environ.get("NEW_DAYS", "90"))
TOP_N = int(os.environ.get("TOP_N", "500"))
OUT_DIR = os.environ.get("OUT_DIR", "/tmp")
STATES = os.environ.get("STATES", "").strip()


async def main() -> int:
    states = [s.strip().upper() for s in STATES.split(",") if s.strip()] if STATES else US_STATES_PLUS
    print(f"[{datetime.now():%H:%M:%S}] National pull")
    print(f"  states:    {len(states)}  ({', '.join(states[:8])}{'...' if len(states)>8 else ''})")
    print(f"  specialty: {SPECIALTY}")
    print(f"  per_state: {PER_STATE}  (max raw = {len(states)*PER_STATE})")
    print(f"  new_only:  {NEW_ONLY}  new_days={NEW_DAYS}")

    t0 = time.time()
    prospects = await prospect_multi_state(
        states=states, specialty=SPECIALTY, per_state=PER_STATE,
        new_only=NEW_ONLY, new_days=NEW_DAYS,
    )
    print(f"[{datetime.now():%H:%M:%S}] NPPES pulled {len(prospects)} unique prospects in {time.time()-t0:.1f}s")

    if not prospects:
        print("NO PROSPECTS — abort"); return 1

    t1 = time.time()
    print(f"[{datetime.now():%H:%M:%S}] Enriching all rows...")
    result = await _enrich_dm_only(prospects)
    rows = result.get("rows") or []
    summary = result.get("summary") or {}
    print(f"[{datetime.now():%H:%M:%S}] Enriched {len(rows)} rows in {time.time()-t1:.1f}s")
    print(f"  summary: {summary}")

    if not rows:
        print("NO ROWS — abort"); return 1

    rows.sort(key=lambda r: -int(r.get("Heat Score") or 0))

    date = datetime.now().strftime("%Y%m%d")
    full_path = os.path.join(OUT_DIR, f"leads_national_{SPECIALTY}_{date}.csv")
    top_path  = os.path.join(OUT_DIR, f"leads_top{TOP_N}_{SPECIALTY}_{date}.csv")

    headers = list(rows[0].keys())
    with open(full_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers); w.writeheader()
        for r in rows: w.writerow(r)
    with open(top_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers); w.writeheader()
        for r in rows[:TOP_N]: w.writerow(r)

    # Per-state breakdown
    by_state: dict[str, int] = {}
    for r in rows:
        s = r.get("State", "??")
        by_state[s] = by_state.get(s, 0) + 1
    print("\n=== PER-STATE LEADS ===")
    for s in sorted(by_state, key=lambda k: -by_state[k]):
        print(f"  {s}: {by_state[s]}")

    # Top 5 console preview
    print("\n=== TOP 5 NATIONAL LEADS ===")
    for i, r in enumerate(rows[:5], 1):
        dm = r.get("Decision Maker") or "(no DM)"
        title = r.get("DM Title") or ""
        org = r.get("Org Name", "")
        addr = r.get("Address", "")
        city = r.get("City", "")
        state = r.get("State", "")
        phone = r.get("Direct Line") or r.get("Phone") or ""
        heat = r.get("Heat Score") or 0
        tier = r.get("Tier") or ""
        print(f"\n#{i}  [Heat {heat} / Tier {tier}]  {state}")
        print(f"    Org:    {org}")
        print(f"    Addr:   {addr}, {city} {state}")
        print(f"    DM:     {dm}  ({title})  ph={phone}")
        if r.get("LinkedIn Search URL"):
            print(f"    LI:     {r['LinkedIn Search URL']}")
        if r.get("Backup Contact"):
            print(f"    Backup: {r['Backup Contact']} ({r.get('Backup Title','')}) ph={r.get('Backup Phone','')}")

    print(f"\n=== DONE in {time.time()-t0:.1f}s ===")
    print(f"  full: {full_path}  ({len(rows)} rows)")
    print(f"  top:  {top_path}  ({min(TOP_N,len(rows))} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
