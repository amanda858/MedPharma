#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_public_data.py

Pulls FREE public data from CMS — no membership, no key required.

1) CLIA labs (full national CSV from data.cms.gov)  -> data/clia_labs.csv
2) NPI orgs by state+taxonomy via NPPES Registry API -> data/npi_registry.csv

Run:
    python3 fetch_public_data.py
or:
    python3 fetch_public_data.py --states FL,TX,CA --max-per-state 500
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

CLIA_URL = "https://data.cms.gov/sites/default/files/2025-09/CLIA-Provider-Information.csv"
CLIA_FALLBACK = "https://data.cms.gov/data-api/v1/dataset/d52fe09e-da2c-4d6a-93b0-69a1ab1f8b5e/data.csv"

NPI_API = "https://npiregistry.cms.hhs.gov/api/?version=2.1"

# Lab + urgent care + ASC taxonomy codes (NUCC)
LAB_TAXONOMIES = [
    "291U00000X",  # Clinical Medical Laboratory
    "291900002X",  # Pathology / molecular
    "293D00000X",  # Physiological Laboratory
    "292200000X",  # Dental Laboratory
    "246Q00000X",  # Anatomic + Clinical Pathology
    "246R00000X",  # Pathology - Clinical
    "247200000X",  # Hematology
    "246W00000X",  # Toxicology
    "246X00000X",  # Cytology
    "246Y00000X",  # Immunology
    "247100000X",  # Medical / Clinical
    "246Z00000X",  # Other
    "261QU0200X",  # Clinic/Center - Urgent Care
    "261QA1903X",  # Clinic - Ambulatory Surgical
]

DEFAULT_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
]


# -------------------------------------------------------------------
# CLIA
# -------------------------------------------------------------------

def fetch_clia(out_path: str) -> int:
    print(f"[CLIA] fetching {CLIA_URL}")
    for url in (CLIA_URL, CLIA_FALLBACK):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as resp, open(out_path, "wb") as f:
                while True:
                    chunk = resp.read(1 << 16)
                    if not chunk:
                        break
                    f.write(chunk)
            size = os.path.getsize(out_path)
            print(f"[CLIA] saved {out_path} ({size:,} bytes)")
            return size
        except Exception as e:
            print(f"[CLIA] failed {url}: {e}")
    return 0


# -------------------------------------------------------------------
# NPI Registry API  (free, no key, 200 results/call)
# -------------------------------------------------------------------

def npi_search(state: str, taxonomy: str, limit: int = 200, skip: int = 0) -> list[dict]:
    params = {
        "version": "2.1",
        "enumeration_type": "NPI-2",
        "state": state,
        "taxonomy_description": taxonomy,
        "limit": limit,
        "skip": skip,
    }
    url = NPI_API + "&" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read().decode("utf-8"))
            return data.get("results", []) or []
    except Exception as e:
        print(f"[NPI] fail state={state} taxonomy={taxonomy}: {e}")
        return []


def flatten_npi(rec: dict) -> dict:
    basic = rec.get("basic", {}) or {}
    addrs = rec.get("addresses", []) or []
    practice = next((a for a in addrs if a.get("address_purpose") == "LOCATION"), addrs[0] if addrs else {})
    taxs = rec.get("taxonomies", []) or []
    primary_tax = next((t for t in taxs if t.get("primary")), taxs[0] if taxs else {})
    return {
        "NPI": rec.get("number", ""),
        "Entity Type Code": str(rec.get("enumeration_type", "")).replace("NPI-", ""),
        "Provider Organization Name (Legal Business Name)": basic.get("organization_name") or basic.get("name") or "",
        "Provider First Line Business Practice Location Address": practice.get("address_1", ""),
        "Provider Business Practice Location Address City Name": practice.get("city", ""),
        "Provider Business Practice Location Address State Name": practice.get("state", ""),
        "Provider Business Practice Location Address Postal Code": practice.get("postal_code", ""),
        "Provider Business Practice Location Address Telephone Number": practice.get("telephone_number", ""),
        "Healthcare Provider Taxonomy Code_1": primary_tax.get("code", ""),
        "Healthcare Provider Taxonomy Description_1": primary_tax.get("desc", ""),
        "Authorized Official Last Name": basic.get("authorized_official_last_name", ""),
        "Authorized Official First Name": basic.get("authorized_official_first_name", ""),
        "Authorized Official Title or Position": basic.get("authorized_official_title_or_position", ""),
    }


def fetch_npi(states: list[str], max_per_state: int, out_path: str) -> int:
    fieldnames = [
        "NPI", "Entity Type Code",
        "Provider Organization Name (Legal Business Name)",
        "Provider First Line Business Practice Location Address",
        "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name",
        "Provider Business Practice Location Address Postal Code",
        "Provider Business Practice Location Address Telephone Number",
        "Healthcare Provider Taxonomy Code_1",
        "Healthcare Provider Taxonomy Description_1",
        "Authorized Official Last Name",
        "Authorized Official First Name",
        "Authorized Official Title or Position",
    ]
    seen: set[str] = set()
    total = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        # NPI Registry doesn't search by code directly; use taxonomy description keywords.
        tax_keywords = [
            "Laboratory", "Clinical Medical Laboratory",
            "Pathology", "Anatomic Pathology", "Clinical Pathology",
            "Molecular Genetic Pathology", "Molecular", "Genomics",
            "Toxicology", "Hematology", "Cytology", "Immunology",
            "Microbiology", "Blood Banking", "Chemistry",
            "Urgent Care", "Ambulatory Surgical",
        ]

        for state in states:
            state_count = 0
            for kw in tax_keywords:
                skip = 0
                while skip < 1200 and state_count < max_per_state:
                    results = npi_search(state, kw, limit=200, skip=skip)
                    if not results:
                        break
                    for rec in results:
                        flat = flatten_npi(rec)
                        npi = flat["NPI"]
                        if not npi or npi in seen:
                            continue
                        seen.add(npi)
                        w.writerow(flat)
                        total += 1
                        state_count += 1
                        if state_count >= max_per_state:
                            break
                    if len(results) < 200:
                        break
                    skip += 200
                    time.sleep(0.15)
                if state_count >= max_per_state:
                    break
            print(f"[NPI] {state}: {state_count} orgs (running total {total})")
    print(f"[NPI] saved {out_path} ({total:,} rows)")
    return total


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--states", default=",".join(DEFAULT_STATES),
                    help=f"comma states, default {','.join(DEFAULT_STATES)}")
    ap.add_argument("--max-per-state", type=int, default=400)
    ap.add_argument("--skip-clia", action="store_true")
    ap.add_argument("--skip-npi", action="store_true")
    args = ap.parse_args()

    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    if not args.skip_clia:
        fetch_clia(os.path.join(DATA_DIR, "clia_labs.csv"))

    if not args.skip_npi:
        fetch_npi(states, args.max_per_state, os.path.join(DATA_DIR, "npi_registry.csv"))

    # Stub the other two so the engine has something to read (optional empty).
    for name in ("state_labs.csv", "cms_denials.csv"):
        p = os.path.join(DATA_DIR, name)
        if not os.path.exists(p):
            with open(p, "w", encoding="utf-8") as f:
                if name == "state_labs.csv":
                    f.write("License Number,Facility Name,Address 1,City,State,Zip,Status,License Type,Issue Date,Expiration Date\n")
                else:
                    f.write("NPI,CLIA,Total Claims,Denied Claims,Denied Amount,Period\n")
            print(f"[STUB] {p} created (empty header — optional source)")

    print("\n[DONE] data/ now has:")
    for n in os.listdir(DATA_DIR):
        p = os.path.join(DATA_DIR, n)
        if os.path.isfile(p):
            try:
                lines = sum(1 for _ in open(p, encoding="utf-8", errors="replace"))
            except Exception:
                lines = "?"
            print(f"  {n:<28} {os.path.getsize(p):>12,} bytes  {lines} lines")


if __name__ == "__main__":
    main()
