#!/usr/bin/env python3
"""Probe CLIA Provider of Services dataset shape."""
import httpx, json

DS = "d3eb38ac-d8e9-40d3-b7b7-6205d3d1dc16"
URL = f"https://data.cms.gov/data-api/v1/dataset/{DS}/data"

print(f"=== Fetching first rows from {URL} ===")
r = httpx.get(URL, params={"size": 3}, timeout=60.0, follow_redirects=True)
print(f"HTTP {r.status_code}, {len(r.content)} bytes")
if r.status_code == 200:
    data = r.json()
    if isinstance(data, list) and data:
        print(f"\nField names ({len(data[0])} cols):")
        for k in data[0].keys():
            print(f"  {k}")
        print(f"\n=== Sample row 0 ===")
        for k, v in data[0].items():
            print(f"  {k} = {v!r}")
    else:
        print(repr(data)[:1000])

# Try filtered query for a known FL lab
print("\n=== Filter on STATE=FL ===")
r = httpx.get(URL, params={"size": 5, "filter[STATE]": "FL"}, timeout=60.0, follow_redirects=True)
print(f"HTTP {r.status_code}")
if r.status_code == 200:
    rows = r.json()
    print(f"rows: {len(rows) if isinstance(rows, list) else 'NA'}")
    if isinstance(rows, list):
        for row in rows[:3]:
            # Print just the most relevant fields
            for k in ("PROVNAME", "FAC_NAME", "ADDRESS", "CITY", "STATE", "ZIP",
                      "DIRECTOR_AFFL", "DIRECTOR_NAME", "OWNER", "DIR_FIRST_NAME", "DIR_LAST_NAME"):
                if k in row:
                    print(f"  {k}: {row[k]}")
            print("  ---")
