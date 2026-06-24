#!/usr/bin/env python3
"""Probe CMS CLIA lab dataset for relevance.

CLIA Lab Demographic data is on data.cms.gov. We need:
  1. A working endpoint
  2. Lab director name field
  3. Address fields to match against NPPES
  4. Reachable from this IP without auth
"""
import httpx, json, sys

# Known CMS public data API base
BASE = "https://data.cms.gov/data-api/v1/dataset"

# CLIA Laboratory Demographic Information dataset UUID
# (public, listed on data.cms.gov/provider-data)
DATASETS = [
    # Try a few candidate UUIDs found in the public CMS catalog
    "7a6e2ec7-4a67-4e8c-83b0-2a5f86d5b497",
    # fallback: search via the metadata API
]

# Try the catalog search first
print("=== 1) Catalog search for 'CLIA' ===")
try:
    r = httpx.get(
        "https://data.cms.gov/data.json",
        timeout=30.0,
        follow_redirects=True,
    )
    print(f"  HTTP {r.status_code}, {len(r.content)} bytes")
    if r.status_code == 200:
        try:
            cat = r.json()
            datasets = cat.get("dataset", [])
            print(f"  total datasets: {len(datasets)}")
            matches = [d for d in datasets if "clia" in (d.get("title", "") + d.get("description", "")).lower()]
            print(f"  CLIA matches: {len(matches)}")
            for m in matches[:10]:
                title = m.get("title", "")
                ident = m.get("identifier", "")
                # find data API URL
                dist = m.get("distribution", []) or []
                api_urls = [d.get("accessURL") or d.get("downloadURL") for d in dist]
                print(f"    - {title}")
                print(f"      id: {ident}")
                for u in api_urls[:3]:
                    if u: print(f"      url: {u}")
        except Exception as e:
            print(f"  json parse failed: {e}")
            print(r.text[:500])
except Exception as e:
    print(f"  failed: {e}")

# Try Socrata API which CMS also exposes
print("\n=== 2) Socrata search ===")
try:
    r = httpx.get(
        "https://data.cms.gov/api/views/metadata/v1?search=clia",
        timeout=30.0,
        follow_redirects=True,
    )
    print(f"  HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f"  results: {len(data)}")
            for d in data[:10]:
                print(f"    - {d.get('name')} (id={d.get('id')})")
except Exception as e:
    print(f"  failed: {e}")
