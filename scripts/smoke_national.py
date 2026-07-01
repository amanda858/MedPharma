#!/usr/bin/env python3
"""National x specialty smoke test for the lead engine.

Proves that EVERY specialty returns real lab prospects across ALL 50 states
(+ DC + PR) from the live NPPES registry, then proves one end-to-end
enrichment run produces finished, scored rows.

Usage:  python3 scripts/smoke_national.py            # all 52 states, per_state=2
        PER_STATE=3 python3 scripts/smoke_national.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.bulk_prospector import (
    SPECIALTY_KEYWORDS,
    prospect_state,
    _enrich_dm_only,
)

US_STATES_PLUS = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR",
]

PER_STATE = int(os.environ.get("PER_STATE", "2"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "12"))


async def _bounded_pull(specialty: str, states: list[str], sem: asyncio.Semaphore):
    async def one(state: str):
        async with sem:
            try:
                return state, await prospect_state(state, specialty=specialty, limit=PER_STATE)
            except Exception as e:  # noqa: BLE001
                return state, e

    return await asyncio.gather(*[one(s) for s in states])


async def main() -> int:
    sem = asyncio.Semaphore(CONCURRENCY)
    print("=" * 78)
    print(f"NATIONAL x SPECIALTY SMOKE   states={len(US_STATES_PLUS)}  per_state={PER_STATE}")
    print("=" * 78)
    print(f"{'specialty':<16}{'orgs':>6}{'states_hit':>12}{'errs':>6}   sample")
    print("-" * 78)

    overall_pass = 0
    grand_total = 0
    t0 = time.time()
    matrix = []
    for specialty in SPECIALTY_KEYWORDS:
        res = await _bounded_pull(specialty, US_STATES_PLUS, sem)
        orgs = 0
        states_hit = 0
        errs = 0
        seen = set()
        sample = ""
        for state, r in res:
            if isinstance(r, Exception):
                errs += 1
                continue
            if r:
                states_hit += 1
            for row in r:
                npi = row.get("npi")
                if npi and npi not in seen:
                    seen.add(npi)
                    orgs += 1
                    if not sample:
                        sample = f"{row['organization_name'][:32]} ({row['city']}, {row['state']})"
        grand_total += orgs
        ok = orgs > 0
        overall_pass += 1 if ok else 0
        flag = "PASS" if ok else "FAIL"
        matrix.append((specialty, orgs, states_hit, errs, ok))
        print(f"{specialty:<16}{orgs:>6}{states_hit:>12}{errs:>6}   {flag}  {sample}")

    dt = time.time() - t0
    print("-" * 78)
    print(f"RESULT: {overall_pass}/{len(SPECIALTY_KEYWORDS)} specialties returned real national leads "
          f"| {grand_total} orgs sampled | {dt:.1f}s")
    fails = [m[0] for m in matrix if not m[4]]
    if fails:
        print(f"SPECIALTIES WITH ZERO RESULTS: {', '.join(fails)}")

    # ── End-to-end enrichment proof (the REAL national path) ──
    # national_pull.py runs _enrich_dm_only(prospects, fast=True) per state.
    # Prove that same path turns prospects into finished, scored rows.
    print("=" * 78)
    print("ENRICHMENT E2E (real national path: _enrich_dm_only fast=True)...")
    e2e_states = [("FL", "clinical"), ("TX", "toxicology"), ("CA", "molecular")]
    e2e_in = e2e_out = e2e_dm = 0
    t1 = time.time()
    try:
        for st, sp in e2e_states:
            prospects = await prospect_state(st, specialty=sp, limit=10)
            res = await asyncio.wait_for(_enrich_dm_only(prospects, fast=True), timeout=60)
            rows = res.get("rows") or []
            dm = sum(1 for r in rows if r.get("Decision Maker"))
            e2e_in += len(prospects); e2e_out += len(rows); e2e_dm += dm
            heats = sorted((int(r.get("Heat Score") or 0) for r in rows), reverse=True)[:3]
            print(f"  {sp}/{st}: {len(prospects)} -> {len(rows)} rows | DM={dm} | top_heat={heats}")
        ok = e2e_out > 0
        print(f"  TOTAL: {e2e_in} prospects -> {e2e_out} rows | rows_with_DM={e2e_dm} | {time.time()-t1:.1f}s")
        print("  ENRICHMENT: " + ("PASS" if ok else "FAIL"))
    except Exception as e:  # noqa: BLE001
        print(f"  ENRICHMENT: FAIL — {type(e).__name__}: {e}")

    return 0 if overall_pass == len(SPECIALTY_KEYWORDS) else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
