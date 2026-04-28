"""National daily lead pull — wired into the scheduler.

Runs at 5 AM EST every day. Pulls all 50 states + DC + PR from NPPES,
enriches with DM + backup + LinkedIn search URLs, writes a CSV to disk,
and stores the row count + path in `lead_pulls` so the UI can surface
"Today's national leads — N rows" at any time.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import sqlite3
import time
from datetime import datetime
from typing import Any

log = logging.getLogger(__name__)

US_STATES_PLUS = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR",
]

OUT_DIR = os.environ.get("NATIONAL_PULL_DIR", "/data/national_pulls")
SPECIALTY = os.environ.get("NATIONAL_PULL_SPECIALTY", "clinical")
PER_STATE = int(os.environ.get("NATIONAL_PULL_PER_STATE", "50"))
NEW_ONLY = os.environ.get("NATIONAL_PULL_NEW_ONLY", "0") == "1"
NEW_DAYS = int(os.environ.get("NATIONAL_PULL_NEW_DAYS", "90"))
DB_PATH = os.environ.get("DB_PATH", "/data/leads.db")

# Daily national pull defaults to high-quality, email-required output.
# Override these env vars at the platform level if a wider net is desired.
os.environ.setdefault("QUALITY_FIRST", "1")
os.environ.setdefault("REQUIRE_EMAIL", "1")
os.environ.setdefault("ENABLE_CLIA_ENRICHMENT", "1")
os.environ.setdefault("ENABLE_PUBMED_LOOKUP", "1")
os.environ.setdefault("ENABLE_EMAIL_ENRICHMENT", "1")


def _ensure_dir(p: str) -> None:
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass


def _record_pull(date_str: str, csv_path: str, row_count: int, summary: dict[str, Any]) -> None:
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS national_pulls("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "pull_date TEXT, specialty TEXT, csv_path TEXT,"
                "row_count INTEGER, summary_json TEXT, created_at INTEGER)"
            )
            import json as _j
            c.execute(
                "INSERT INTO national_pulls(pull_date, specialty, csv_path, row_count, summary_json, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (date_str, SPECIALTY, csv_path, row_count, _j.dumps(summary), int(time.time())),
            )
    except Exception as e:
        log.warning(f"Could not record national pull metadata: {e}")


async def _run_pull_async() -> dict[str, Any]:
    from app.bulk_prospector import prospect_multi_state, _enrich_dm_only

    t0 = time.time()
    log.info(f"[national-pull] start specialty={SPECIALTY} per_state={PER_STATE} states={len(US_STATES_PLUS)}")

    prospects = await prospect_multi_state(
        states=US_STATES_PLUS, specialty=SPECIALTY,
        per_state=PER_STATE, new_only=NEW_ONLY, new_days=NEW_DAYS,
    )
    log.info(f"[national-pull] NPPES returned {len(prospects)} unique prospects in {time.time()-t0:.1f}s")
    if not prospects:
        return {"ok": False, "reason": "no prospects"}

    t1 = time.time()
    result = await _enrich_dm_only(prospects)
    rows = result.get("rows") or []
    summary = result.get("summary") or {}
    log.info(f"[national-pull] enriched {len(rows)} rows in {time.time()-t1:.1f}s — {summary}")
    if not rows:
        return {"ok": False, "reason": "no enriched rows", "summary": summary}

    rows.sort(key=lambda r: -int(r.get("Heat Score") or 0))

    _ensure_dir(OUT_DIR)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(OUT_DIR, f"leads_national_{SPECIALTY}_{date_str}.csv")

    headers = list(rows[0].keys())
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    _record_pull(date_str, csv_path, len(rows), summary)
    log.info(f"[national-pull] wrote {len(rows)} rows -> {csv_path}  total {time.time()-t0:.1f}s")
    return {"ok": True, "csv_path": csv_path, "row_count": len(rows), "summary": summary}


def run_national_pull_job() -> None:
    """Synchronous entrypoint for APScheduler."""
    try:
        asyncio.run(_run_pull_async())
    except RuntimeError:
        # Already-running loop fallback
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_run_pull_async())
        finally:
            loop.close()
    except Exception as e:
        log.exception(f"national pull job failed: {e}")
