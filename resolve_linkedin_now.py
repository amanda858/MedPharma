#!/usr/bin/env python3
"""Bulk LinkedIn profile resolver — run this ONCE to populate the cache.

Reads all named decision makers from the database, resolves each to a
real linkedin.com/in/<slug> URL, and stores results in the local cache.

After this runs, hunt_now.py will use cached URLs instantly — no HTTP calls.

Usage:
    python3 resolve_linkedin_now.py            # resolve all unresolved
    python3 resolve_linkedin_now.py --limit 50 # resolve first 50 only
    python3 resolve_linkedin_now.py --reset    # clear cache and start fresh
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from app.config import DATABASE_PATH
from app.linkedin_resolver import (
    CACHE_DB,
    _cache_get,
    _cache_put,
    _norm_key,
    resolve_linkedin_profile,
    linkedin_search_url,
    _clean_org,
)


def _get_named_leads() -> list[dict]:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT
            le.first_name, le.last_name, sl.organization_name, sl.npi
        FROM lead_emails le
        JOIN saved_leads sl ON le.npi = sl.npi
        WHERE le.first_name != '' AND le.last_name != ''
        ORDER BY sl.lead_score DESC
    """).fetchall()
    # Also grab leads from national CSV with known names but no email
    conn.close()
    return [dict(r) for r in rows]


def _is_cached(first: str, last: str, org: str) -> bool:
    key = _norm_key(first, last, org)
    return _cache_get("linkedin", key) is not None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Max profiles to resolve (0 = all)")
    parser.add_argument("--reset", action="store_true", help="Clear cache before resolving")
    args = parser.parse_args()

    if args.reset:
        conn = sqlite3.connect(CACHE_DB)
        conn.execute("DELETE FROM profile_cache WHERE platform='linkedin'")
        conn.commit()
        conn.close()
        print("Cache cleared.")

    leads = _get_named_leads()
    unresolved = [l for l in leads if not _is_cached(l["first_name"], l["last_name"], l["organization_name"])]
    total = len(unresolved)
    if args.limit:
        unresolved = unresolved[:args.limit]

    print(f"LinkedIn Profile Resolver")
    print(f"{'='*50}")
    print(f"Named leads in DB  : {len(leads)}")
    print(f"Already cached     : {len(leads) - total}")
    print(f"To resolve now     : {len(unresolved)}")
    if args.limit:
        print(f"Limited to         : {args.limit}")
    print(f"Cache DB           : {CACHE_DB}")
    print()

    resolved = 0
    not_found = 0
    errors = 0

    for i, lead in enumerate(unresolved, 1):
        first = lead["first_name"].strip()
        last = lead["last_name"].strip()
        org = lead["organization_name"].strip()
        name = f"{first} {last}"

        try:
            url = resolve_linkedin_profile(first, last, org)
            if url:
                resolved += 1
                print(f"[{i}/{len(unresolved)}] ✅ {name:30s} → {url}")
            else:
                not_found += 1
                fallback = linkedin_search_url(first, last, org)
                print(f"[{i}/{len(unresolved)}] 🔍 {name:30s} → no direct profile (search: {fallback[:60]}...)")
        except Exception as e:
            errors += 1
            print(f"[{i}/{len(unresolved)}] ❌ {name:30s} → error: {e}")

        # Progress summary every 25
        if i % 25 == 0:
            print(f"\n--- Progress: {resolved} resolved, {not_found} not found, {errors} errors ---\n")

    print()
    print(f"{'='*50}")
    print(f"Done.")
    print(f"Resolved to direct profile : {resolved}")
    print(f"Not found (search URL used): {not_found}")
    print(f"Errors                     : {errors}")
    print()
    print("Now run: python3 hunt_now.py")
    print("Your output CSV will have real linkedin.com/in/ URLs for every resolved profile.")


if __name__ == "__main__":
    main()
