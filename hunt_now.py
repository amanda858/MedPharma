#!/usr/bin/env python3
"""Simple one-command outreach builder.

Builds one merged CSV for immediate use:
  1. real person-email rows first
  2. LinkedIn fallback rows second
"""

from __future__ import annotations

import csv
import os

import build_real_human_email_export as builder
from app.database import init_db, save_outreach_queue
from app.linkedin_resolver import linkedin_company_people_url, linkedin_company_search_url, linkedin_search_url


ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "output")
HUNT_NOW_CSV = os.path.join(OUT, "HUNT_NOW.csv")
HUNT_NOW_TOP_100_CSV = os.path.join(OUT, "HUNT_NOW_top_100.csv")


def _email_rows() -> list[dict]:
    rows = builder._apply_mx_gate(builder._best_rows())
    out: list[dict] = []
    for row in rows:
        org_name = row.get("Org Name", "")
        decision_maker = row.get("Decision Maker", "")
        title = row.get("DM Title", "")
        first_name = ""
        last_name = ""
        if decision_maker:
            parts = [p for p in str(decision_maker).split() if p]
            if parts:
                first_name = parts[0]
                last_name = parts[-1] if len(parts) > 1 else ""
        linkedin = linkedin_search_url(first_name, last_name, org_name) if first_name and last_name else ""
        company_linkedin = linkedin_company_search_url(org_name) if org_name else ""
        company_people = linkedin_company_people_url(org_name) if org_name else ""
        out.append({
            "Primary Action": "email first, linkedin backup",
            "Outreach Channel": "email",
            "Heat Score": row.get("Heat Score", ""),
            "Tier": row.get("Tier", ""),
            "Priority": row.get("Priority", ""),
            "Org Name": org_name,
            "Decision Maker": decision_maker,
            "Title": title,
            "Email": row.get("DM Email", ""),
            "Email Source": row.get("DM Email Source", ""),
            "Email Verdict": row.get("DM Email Verdict", ""),
            "LinkedIn": linkedin,
            "Company LinkedIn": company_linkedin,
            "Company People Search": company_people,
            "Phone": row.get("Phone", ""),
            "City": row.get("City", ""),
            "State": row.get("State", ""),
            "NPI": row.get("NPI", ""),
            "Notes": row.get("Notes", ""),
        })
    return out


def _linkedin_rows(email_rows: list[dict]) -> list[dict]:
    fallback_rows = builder._build_linkedin_fallback_rows(email_rows)
    out: list[dict] = []
    for row in fallback_rows:
        person_name = row.get("Known Contact", "") or row.get("Backup Person", "")
        person_title = row.get("Known Contact Title", "") or row.get("Backup Person Title", "")
        person_linkedin = row.get("Known Contact LinkedIn", "") or row.get("Backup Person LinkedIn", "")
        if not person_linkedin:
            person_linkedin = row.get("Employee LinkedIn 1", "")
        out.append({
            "Primary Action": "linkedin first",
            "Outreach Channel": "linkedin",
            "Heat Score": row.get("Heat Score", ""),
            "Tier": row.get("Tier", ""),
            "Priority": row.get("Priority", ""),
            "Org Name": row.get("Org Name", ""),
            "Decision Maker": person_name,
            "Title": person_title,
            "Email": "",
            "Email Source": "",
            "Email Verdict": "",
            "LinkedIn": person_linkedin,
            "Company LinkedIn": row.get("Company LinkedIn", ""),
            "Company People Search": row.get("Company People Search", ""),
            "Phone": row.get("Phone", ""),
            "City": row.get("City", ""),
            "State": row.get("State", ""),
            "NPI": row.get("NPI", ""),
            "Notes": row.get("Notes", ""),
        })
    return out


def _sort_key(row: dict) -> tuple[int, int, str]:
    channel_rank = 0 if row.get("Outreach Channel") == "email" else 1
    verdict_rank = {
        "deliverable": 0,
        "catch-all": 1,
        "risky": 2,
        "unknown": 3,
        "": 4,
    }.get(str(row.get("Email Verdict") or "").strip().lower(), 4)
    try:
        heat = int(row.get("Heat Score") or 0)
    except Exception:
        heat = 0
    return (channel_rank, verdict_rank, -heat, str(row.get("Org Name") or ""))


def _write(path: str, rows: list[dict]) -> None:
    fieldnames = [
        "Primary Action",
        "Outreach Channel",
        "Heat Score",
        "Tier",
        "Priority",
        "Org Name",
        "Decision Maker",
        "Title",
        "Email",
        "Email Source",
        "Email Verdict",
        "LinkedIn",
        "Company LinkedIn",
        "Company People Search",
        "Phone",
        "City",
        "State",
        "NPI",
        "Notes",
    ]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    init_db()
    email_rows = _email_rows()
    linkedin_rows = _linkedin_rows(email_rows)
    merged = sorted(email_rows + linkedin_rows, key=_sort_key)
    _write(HUNT_NOW_CSV, merged)
    _write(HUNT_NOW_TOP_100_CSV, merged[:100])
    run_meta = save_outreach_queue(
        merged,
        run_type="hunt_now",
        notes=f"email_rows={len(email_rows)} linkedin_rows={len(linkedin_rows)} top_file={os.path.basename(HUNT_NOW_TOP_100_CSV)}",
    )
    print(f"Wrote {len(email_rows)} email rows")
    print(f"Wrote {len(linkedin_rows)} LinkedIn fallback rows")
    print(f"Wrote merged hunt file: {HUNT_NOW_CSV}")
    print(f"Wrote top 100 hunt file: {HUNT_NOW_TOP_100_CSV}")
    print(f"Saved outreach queue run {run_meta['run_id']} with {run_meta['row_count']} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())