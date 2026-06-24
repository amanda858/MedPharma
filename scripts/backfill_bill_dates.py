#!/usr/bin/env python3
"""Backfill Bill Dates for already-imported billed claims.

WHY: Older imports stored billed/submitted claims with a blank BillDate because
the source file shipped no bill-date column. Every dated billed/production view
(Billed Activity, All-Time Billed, the Team Production "$ Billed" column, AR
aging) keys off BillDate, so those claims read $0 even though their status said
they had gone out the door. The importer now stamps a BillDate on import; this
script fixes the rows that were already saved WITHOUT needing a redeploy.

WHAT IT DOES: For every claim whose status is past the pre-bill stages
(Intake / Verification / Coding) and whose BillDate is blank, set BillDate to
the service date (DOS) when present, otherwise the row's creation date. This
mirrors the importer's DOS-first fallback so historical claims keep a realistic
timeline.

USAGE (Render Shell or locally):
    DB_PATH=/data/leads.db python3 scripts/backfill_bill_dates.py            # apply
    DB_PATH=/data/leads.db python3 scripts/backfill_bill_dates.py --dry-run  # preview only
"""
from __future__ import annotations

import os
import sqlite3
import sys

PRE_BILL_STATUSES = ("Intake", "Verification", "Coding")

# Stamp DOS when it parses to a real date, else the row's creation date,
# truncated to YYYY-MM-DD. Only touches billed rows with a blank BillDate.
_UPDATE_SQL = """
UPDATE claims_master
   SET BillDate = substr(
           COALESCE(NULLIF(TRIM(DOS), ''), date(created_at), date('now')), 1, 10
       ),
       updated_at = CURRENT_TIMESTAMP
 WHERE TRIM(COALESCE(BillDate, '')) = ''
   AND TRIM(COALESCE(ClaimStatus, '')) NOT IN ({placeholders})
""".format(placeholders=",".join("?" for _ in PRE_BILL_STATUSES))

_COUNT_SQL = """
SELECT COUNT(*)
  FROM claims_master
 WHERE TRIM(COALESCE(BillDate, '')) = ''
   AND TRIM(COALESCE(ClaimStatus, '')) NOT IN ({placeholders})
""".format(placeholders=",".join("?" for _ in PRE_BILL_STATUSES))


def backfill(db_path: str, dry_run: bool = False) -> int:
    conn = sqlite3.connect(db_path)
    try:
        affected = conn.execute(_COUNT_SQL, PRE_BILL_STATUSES).fetchone()[0]
        print(f"Billed claims missing a Bill Date: {affected}")
        if dry_run:
            print("Dry run — no changes written.")
            return affected
        conn.execute(_UPDATE_SQL, PRE_BILL_STATUSES)
        conn.commit()
        print(f"Stamped Bill Dates on {affected} claim(s).")
        return affected
    finally:
        conn.close()


def main(argv) -> int:
    db_path = os.environ.get("DB_PATH", "data/leads.db")
    dry_run = "--dry-run" in argv
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}", file=sys.stderr)
        return 1
    print(f"Using database: {db_path}")
    backfill(db_path, dry_run=dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
