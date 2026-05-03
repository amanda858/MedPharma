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

# Daily national pull defaults to high-quality output.
# QUALITY_FIRST keeps rows with at least a live website, CLIA, or any email.
# REQUIRE_EMAIL is OFF by default so rows with a confirmed domain but no scraped
# email still appear — this dramatically increases usable output when email
# enrichment partially fails (e.g. Hunter key not set, scraping timeouts).
# Set REQUIRE_EMAIL=1 in your platform env vars if you want email-only rows.
os.environ.setdefault("QUALITY_FIRST", "1")
os.environ.setdefault("REQUIRE_EMAIL", "0")
os.environ.setdefault("ENABLE_CLIA_ENRICHMENT", "1")
os.environ.setdefault("ENABLE_PUBMED_LOOKUP", "1")
os.environ.setdefault("ENABLE_EMAIL_ENRICHMENT", "1")


def _ensure_dir(p: str) -> None:
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass


def _record_pull(date_str: str, csv_path: str, row_count: int, summary: dict[str, Any], specialty: str = "") -> None:
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
                (date_str, specialty or SPECIALTY, csv_path, row_count, _j.dumps(summary), int(time.time())),
            )
    except Exception as e:
        log.warning(f"Could not record national pull metadata: {e}")


async def _run_pull_async(
    *,
    states: list[str] | None = None,
    per_state: int | None = None,
    specialty: str | None = None,
) -> dict[str, Any]:
    from app.bulk_prospector import prospect_multi_state, _enrich_dm_only

    use_states = [s.upper() for s in states] if states else US_STATES_PLUS
    use_per_state = int(per_state) if per_state else PER_STATE
    use_specialty = (specialty or SPECIALTY).strip()

    t0 = time.time()
    log.info(f"[national-pull] start specialty={use_specialty} per_state={use_per_state} states={len(use_states)}")
    _ensure_dir(OUT_DIR)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(OUT_DIR, f"leads_national_{use_specialty}_{date_str}.csv")

    all_rows: list[dict] = []
    headers: list[str] = []
    summary_total: dict[str, Any] = {}
    states_done: list[str] = []

    for st in use_states:
        try:
            ts = time.time()
            prospects = await prospect_multi_state(
                states=[st], specialty=use_specialty,
                per_state=use_per_state, new_only=NEW_ONLY, new_days=NEW_DAYS,
            )
            if not prospects:
                log.info(f"[national-pull] {st}: 0 prospects")
                states_done.append(st)
                continue
            res = await _enrich_dm_only(prospects)
            rows = res.get("rows") or []
            summ = res.get("summary") or {}
            log.info(f"[national-pull] {st}: {len(prospects)} prospects -> {len(rows)} rows in {time.time()-ts:.1f}s")
            if rows:
                all_rows.extend(rows)
                if not headers:
                    headers = list(rows[0].keys())
                # Checkpoint: rewrite CSV + record after every state
                all_rows.sort(key=lambda r: -int(r.get("Heat Score") or 0))
                # Ensure consistent header set (union)
                hset = list(headers)
                for r in all_rows:
                    for k in r.keys():
                        if k not in hset:
                            hset.append(k)
                headers = hset
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                    w.writeheader()
                    for r in all_rows:
                        w.writerow(r)
                # Merge summary counters
                for k, v in summ.items():
                    if isinstance(v, (int, float)):
                        summary_total[k] = summary_total.get(k, 0) + v
                _record_pull(date_str, csv_path, len(all_rows), summary_total, use_specialty)
            states_done.append(st)
        except Exception as e:
            log.exception(f"[national-pull] state {st} failed: {e}")

    log.info(f"[national-pull] DONE {len(states_done)}/{len(use_states)} states, "
             f"{len(all_rows)} rows -> {csv_path} total {time.time()-t0:.1f}s")
    if not all_rows:
        return {"ok": False, "reason": "no enriched rows", "states_done": states_done}
    return {"ok": True, "csv_path": csv_path, "row_count": len(all_rows),
            "summary": summary_total, "states_done": states_done}


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


def ensure_seed_loaded() -> dict[str, Any]:
    """If no pull has been recorded, copy the bundled seed CSV into OUT_DIR
    and record it in `national_pulls` so the search UI has data to show.

    The seed is a real, locally-enriched FL/laboratory pull (~28 rows) committed
    in `app/seed/seed_national_pull.csv`. Idempotent — does nothing if any row
    already exists in the table.
    """
    try:
        _ensure_dir(OUT_DIR)
        with sqlite3.connect(DB_PATH, timeout=10) as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS national_pulls("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "pull_date TEXT, specialty TEXT, csv_path TEXT,"
                "row_count INTEGER, summary_json TEXT, created_at INTEGER)"
            )
            row = c.execute("SELECT COUNT(*) FROM national_pulls").fetchone()
            if row and row[0] > 0:
                return {"ok": True, "skipped": True, "reason": "pull already recorded"}

        seed_src = os.path.join(os.path.dirname(__file__), "seed", "seed_national_pull.csv")
        if not os.path.exists(seed_src):
            return {"ok": False, "reason": f"seed file missing at {seed_src}"}

        date_str = datetime.now().strftime("%Y%m%d")
        seed_dst = os.path.join(OUT_DIR, f"leads_national_seed_{date_str}.csv")
        # copy once, count rows
        import shutil
        shutil.copyfile(seed_src, seed_dst)
        row_count = 0
        with open(seed_dst, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for _ in reader:
                row_count += 1
        _record_pull(date_str, seed_dst, row_count, {"source": "seed"}, "seed")
        log.info(f"[national-pull] SEED loaded: {row_count} rows -> {seed_dst}")
        return {"ok": True, "loaded": True, "row_count": row_count, "csv_path": seed_dst}
    except Exception as e:
        log.exception(f"ensure_seed_loaded failed: {e}")
        return {"ok": False, "error": str(e)}

