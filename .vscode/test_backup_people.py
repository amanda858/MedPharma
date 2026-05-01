#!/usr/bin/env python3
"""Test the NPPES backup-person fallback locally."""
import asyncio, sys, os
sys.path.insert(0, "/workspaces/CVOPro")
from app.backup_people import find_backup_people

CASES = [
    # zip, city, state, street, exclude_npi, label
    ("33126", "MIAMI", "FL", "", "", "Miami FL clinical lab area"),
    ("34994", "STUART", "FL", "", "", "Stuart FL (1Lab Diagnostics)"),
    ("34677", "OLDSMAR", "FL", "", "", "Oldsmar FL (24-7 Labs)"),
    ("32401", "PANAMA CITY", "FL", "", "", "Panama City FL (90 Min Lab)"),
]

async def main():
    for zc, city, state, street, excl, label in CASES:
        print(f"\n=== {label}  ({zc} {city} {state}) ===")
        try:
            people = await find_backup_people(
                zip_code=zc, city=city, state=state,
                street_address=street,
                exclude_npi=excl, limit=5,
            )
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            continue
        if not people:
            print("  (no backup people found)")
            continue
        for p in people:
            name = f"{p.get('first','')} {p.get('last','')}".strip()
            print(f"  - NPI {p.get('npi')}  {name:<30}  {p.get('title','') or p.get('taxonomy','')}  ph={p.get('phone','')}")

if __name__ == "__main__":
    asyncio.run(main())
