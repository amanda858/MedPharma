#!/usr/bin/env python3
"""Probe PubMed E-utilities for corresponding author emails."""
import httpx, re

ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# Try a known clinical lab
queries = [
    "Quest Diagnostics[Affiliation]",
    "LabCorp[Affiliation]",
    "Advanced Genetics[Affiliation] AND Miami[Affiliation]",
]

for q in queries:
    print(f"\n=== Query: {q} ===")
    r = httpx.get(ESEARCH, params={
        "db": "pubmed",
        "term": q,
        "retmode": "json",
        "retmax": 5,
        "sort": "date",
    }, timeout=30.0)
    print(f"  esearch HTTP {r.status_code}")
    if r.status_code != 200:
        continue
    ids = (r.json().get("esearchresult", {}).get("idlist") or [])[:3]
    print(f"  ids: {ids}")
    if not ids:
        continue
    r = httpx.get(EFETCH, params={
        "db": "pubmed",
        "id": ",".join(ids),
        "retmode": "xml",
    }, timeout=30.0)
    print(f"  efetch HTTP {r.status_code}, {len(r.content)} bytes")
    if r.status_code != 200:
        continue
    xml = r.text
    # Extract author + email pairs
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", xml)
    affs = re.findall(r"<Affiliation>([^<]+)</Affiliation>", xml)
    print(f"  emails found: {len(emails)}")
    for e in emails[:5]:
        print(f"    {e}")
    print(f"  affiliations sampled: {len(affs)}")
    for a in affs[:3]:
        print(f"    {a[:140]}")
