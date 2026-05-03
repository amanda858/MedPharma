#!/usr/bin/env python3
"""Build a strict marketing export containing only person-level emails."""

from __future__ import annotations

import asyncio
import csv
import os
import re
import sqlite3
from collections import Counter

from app.backup_people import find_backup_people
from app.config import DATABASE_PATH
from app.email_finder import _is_generic_company_mailbox, _is_quality_email
from app.email_verifier import verify_batch
from app.linkedin_resolver import (
    linkedin_company_search_url,
    linkedin_search_url,
    linkedin_company_people_url,
)
from rule_intercept import score_lab_lead


ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "output")
NATIONAL_CSV = os.path.join(OUT, "leads_national.csv")
STRICT_EXPORT = os.path.join(OUT, "FINAL_real_human_emails.csv")
STRICT_TOP_50 = os.path.join(OUT, "FINAL_real_human_top_50.csv")
LINKEDIN_FALLBACK_EXPORT = os.path.join(OUT, "FINAL_linkedin_fallback.csv")
LINKEDIN_FALLBACK_TOP_50 = os.path.join(OUT, "FINAL_linkedin_fallback_top_50.csv")
MAX_LINKEDIN_CANDIDATES_TO_SCAN = 500
MAX_LINKEDIN_FALLBACK_ROWS = 200


SOURCE_RANK = {
    "hunter.io/email-finder": 100,
    "hunter.io/domain-search": 90,
    "pattern_smtp_verified": 85,
    "mx_verified_pattern": 80,
    "website_scrape": 70,
    "national_csv": 55,
}


def _load_national_rows() -> dict[str, dict]:
    if not os.path.exists(NATIONAL_CSV):
        return {}

    out: dict[str, dict] = {}
    with open(NATIONAL_CSV, "r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            npi = str(row.get("npi", "") or "").strip()
            if npi:
                out[npi] = row
    return out


def _source_rank(source: str) -> int:
    return SOURCE_RANK.get((source or "").strip(), 40)


def _parse_tier(value: str) -> str:
    raw = (value or "").strip().upper()
    if raw in {"A", "B", "C"}:
        return raw
    if raw.startswith("TIER ") and raw[-1:] in {"A", "B", "C"}:
        return raw[-1]
    return ""


def _tier_from_saved_lead(tags: str, notes: str) -> str:
    tags_lower = (tags or "").strip().lower()
    notes_upper = (notes or "").strip().upper()
    for tier in ("A", "B", "C"):
        if f"tier-{tier.lower()}" in tags_lower:
            return tier
    match = re.search(r"\bTIER\s+([ABC])\b", notes_upper)
    if match:
        return match.group(1)
    return ""


def _tier_bonus(tier: str) -> int:
    return {"A": 40, "B": 20, "C": 0}.get((tier or "").strip().upper(), 0)


def _default_priority(tier: str) -> str:
    return {"A": "High", "B": "Medium", "C": "Low"}.get((tier or "").strip().upper(), "Low")


def _priority_rank(priority: str) -> int:
    return {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get((priority or "").strip().upper(), 0)


def _name_match_score(email: str, first_name: str, last_name: str) -> int:
    if "@" not in email:
        return 0
    local = email.split("@", 1)[0].lower()
    first = (first_name or "").strip().lower()
    last = (last_name or "").strip().lower()
    score = 0
    if first and first in local:
        score += 8
    if last and last in local:
        score += 8
    if first and last and (f"{first}.{last}" in local or f"{first}{last}" in local):
        score += 8
    return score


def _candidate_rank(row: sqlite3.Row) -> tuple[int, int, int, int]:
    email = str(row["email"] or "").strip().lower()
    confidence = int(row["confidence"] or 0)
    has_name = 1 if (row["first_name"] or row["last_name"]) else 0
    return (
        _source_rank(str(row["source"] or "")),
        confidence,
        _name_match_score(email, str(row["first_name"] or ""), str(row["last_name"] or "")),
        has_name,
    )


def _best_rows() -> list[dict]:
    national = _load_national_rows()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            sl.npi,
            sl.organization_name,
            sl.city,
            sl.state,
            sl.phone,
            sl.lead_score,
            sl.tags,
            sl.notes,
            sl.taxonomy_desc,
            le.email,
            le.first_name,
            le.last_name,
            le.position,
            le.confidence,
            le.source,
            le.domain
        FROM saved_leads sl
        JOIN lead_emails le ON le.npi = sl.npi
        ORDER BY sl.npi, le.confidence DESC, le.id ASC
        """
    ).fetchall()
    conn.close()

    best_person: dict[str, sqlite3.Row] = {}
    best_generic: dict[str, sqlite3.Row] = {}

    for row in rows:
        npi = str(row["npi"] or "").strip()
        email = str(row["email"] or "").strip().lower()
        if not npi or not email:
            continue

        if _is_quality_email(email):
            current = best_person.get(npi)
            if current is None or _candidate_rank(row) > _candidate_rank(current):
                best_person[npi] = row
            continue

        if _is_generic_company_mailbox(email):
            current = best_generic.get(npi)
            current_score = int(current["confidence"] or 0) if current is not None else -1
            if current is None or int(row["confidence"] or 0) > current_score:
                best_generic[npi] = row

    output_rows: list[dict] = []
    seen_org_keys: set[tuple[str, str, str]] = set()
    for npi, person in best_person.items():
        lead = national.get(npi, {})
        generic = best_generic.get(npi)

        org_name = str(lead.get("org_name") or person["organization_name"] or "").strip()
        city = str(lead.get("city") or person["city"] or "").strip()
        state = str(lead.get("state") or person["state"] or "").strip().upper()
        dedupe_key = (org_name.lower(), city.lower(), state.lower())
        if dedupe_key in seen_org_keys or not org_name:
            continue
        seen_org_keys.add(dedupe_key)

        try:
            base_score = int(lead.get("score") or person["lead_score"] or 0)
        except Exception:
            base_score = 0
        tier = _parse_tier(str(lead.get("tier") or "")) or _tier_from_saved_lead(
            str(person["tags"] or ""),
            str(person["notes"] or ""),
        )
        if not tier:
            tier = str(
                score_lab_lead(
                    org_name,
                    lab_type=str(lead.get("taxonomy") or person["taxonomy_desc"] or ""),
                    state=state,
                ).get("tier", "") or ""
            ).strip().upper()
        heat = base_score + _tier_bonus(tier)
        priority = str(lead.get("priority") or "").strip() or _default_priority(tier)

        first_name = str(person["first_name"] or "").strip()
        last_name = str(person["last_name"] or "").strip()
        decision_maker = " ".join(part for part in [first_name, last_name] if part).strip()

        output_rows.append({
            "Heat Score": heat,
            "Lead Score": base_score,
            "Tier": tier,
            "Priority": priority,
            "Org Name": org_name,
            "Taxonomy / Type": str(lead.get("taxonomy") or person["taxonomy_desc"] or "").strip(),
            "NPI": npi,
            "City": city,
            "State": state,
            "ZIP": str(lead.get("zip") or "").strip(),
            "Phone": str(lead.get("phone") or person["phone"] or "").strip(),
            "Decision Maker": decision_maker,
            "DM Title": str(person["position"] or lead.get("contact_title") or "").strip(),
            "DM Email": str(person["email"] or "").strip().lower(),
            "DM Email Confidence": int(person["confidence"] or 0),
            "DM Email Source": str(person["source"] or "").strip(),
            "Company Email": str(generic["email"] or "").strip().lower() if generic is not None else "",
            "Org Domain": str(person["domain"] or lead.get("domain") or "").strip().lower(),
            "Notes": str(person["notes"] or "").strip(),
            "Tags": str(person["tags"] or "").strip(),
        })

    output_rows.sort(
        key=lambda row: (
            -int(row["Heat Score"] or 0),
            -int(row["DM Email Confidence"] or 0),
            row["Org Name"],
        )
    )
    return output_rows


def _write_csv(path: str, rows: list[dict]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _apply_mx_gate(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows

    emails = [str(row.get("DM Email") or "").strip().lower() for row in rows]
    results = asyncio.run(verify_batch(emails, do_smtp=True, concurrency=6))
    by_email = {str(result.get("email") or "").strip().lower(): result for result in results}

    gated: list[dict] = []
    for row in rows:
        email = str(row.get("DM Email") or "").strip().lower()
        verdict = by_email.get(email, {})
        if not verdict.get("mx_found"):
            continue
        row = dict(row)
        row["DM Email MX"] = "true"
        row["DM Email Verdict"] = str(verdict.get("verdict") or "")
        gated.append(row)
    return gated


def _saved_lead_candidates() -> list[sqlite3.Row]:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            npi,
            organization_name,
            first_name,
            last_name,
            credential,
            taxonomy_desc,
            address_line1,
            city,
            state,
            zip_code,
            phone,
            lead_score,
            notes,
            tags
        FROM saved_leads
        WHERE organization_name != ''
        ORDER BY lead_score DESC, updated_at DESC, created_at DESC
        """
    ).fetchall()
    conn.close()
    return rows


def _fallback_contact_title(lead: sqlite3.Row, national: dict) -> str:
    title = str(national.get("contact_title") or "").strip()
    if title:
        return title
    return str(lead["credential"] or "").strip()


def _build_linkedin_fallback_rows(email_rows: list[dict]) -> list[dict]:
    national = _load_national_rows()
    covered_npis = {str(row.get("NPI") or "").strip() for row in email_rows}
    fallback_rows: list[dict] = []
    seen_org_keys: set[tuple[str, str, str]] = set()

    for lead in _saved_lead_candidates()[:MAX_LINKEDIN_CANDIDATES_TO_SCAN]:
        npi = str(lead["npi"] or "").strip()
        if not npi or npi in covered_npis:
            continue

        org_name = str(lead["organization_name"] or "").strip()
        city = str(lead["city"] or "").strip()
        state = str(lead["state"] or "").strip().upper()
        if not org_name:
            continue

        dedupe_key = (org_name.lower(), city.lower(), state.lower())
        if dedupe_key in seen_org_keys:
            continue

        raw_national = national.get(npi, {})
        tier = _parse_tier(str(raw_national.get("tier") or "")) or _tier_from_saved_lead(
            str(lead["tags"] or ""),
            str(lead["notes"] or ""),
        )
        if not tier:
            tier = str(
                score_lab_lead(
                    org_name,
                    lab_type=str(raw_national.get("taxonomy") or lead["taxonomy_desc"] or ""),
                    state=state,
                ).get("tier", "") or ""
            ).strip().upper()
        priority = str(raw_national.get("priority") or "").strip() or _default_priority(tier)
        base_score = int(lead["lead_score"] or 0)
        heat = base_score + _tier_bonus(tier)

        first_name = str(raw_national.get("contact_first") or lead["first_name"] or "").strip()
        last_name = str(raw_national.get("contact_last") or lead["last_name"] or "").strip()
        known_contact = " ".join(part for part in [first_name, last_name] if part).strip()
        known_title = _fallback_contact_title(lead, raw_national)

        named_linkedin = linkedin_search_url(first_name, last_name, org_name) if known_contact else ""

        backup_name = ""
        backup_title = ""
        backup_linkedin = ""
        if not named_linkedin:
            try:
                backups = asyncio.run(
                    find_backup_people(
                        zip_code=str(lead["zip_code"] or ""),
                        city=city,
                        state=state,
                        street_address=str(lead["address_line1"] or ""),
                        exclude_npi=npi,
                        limit=5,
                    )
                )
            except Exception:
                backups = []

            for backup in backups:
                candidate_url = linkedin_search_url(
                    str(backup.get("first") or ""),
                    str(backup.get("last") or ""),
                    org_name,
                )
                if candidate_url:
                    backup_name = " ".join(
                        part for part in [str(backup.get("first") or "").strip(), str(backup.get("last") or "").strip()]
                        if part
                    ).strip()
                    backup_title = str(backup.get("taxonomy") or backup.get("title") or "").strip()
                    backup_linkedin = candidate_url
                    break

        employee_urls: list[str] = []
        if not (named_linkedin or backup_linkedin):
            generic_roles = [
                "owner", "president", "ceo", "cfo", "coo", "director", "administrator", "manager",
            ]
            employee_urls = [
                linkedin_search_url(role, org_name, org_name)
                for role in generic_roles[:3]
            ]
        company_linkedin = linkedin_company_search_url(org_name)
        people_search = linkedin_company_people_url(org_name)

        if not (named_linkedin or backup_linkedin or employee_urls or company_linkedin or people_search):
            continue

        seen_org_keys.add(dedupe_key)
        fallback_rows.append({
            "Heat Score": heat,
            "Lead Score": base_score,
            "Tier": tier,
            "Priority": priority,
            "Org Name": org_name,
            "Taxonomy / Type": str(raw_national.get("taxonomy") or lead["taxonomy_desc"] or "").strip(),
            "NPI": npi,
            "City": city,
            "State": state,
            "ZIP": str(lead["zip_code"] or raw_national.get("zip") or "").strip(),
            "Phone": str(raw_national.get("phone") or lead["phone"] or "").strip(),
            "Known Contact": known_contact,
            "Known Contact Title": known_title,
            "Known Contact LinkedIn": named_linkedin,
            "Backup Person": backup_name,
            "Backup Person Title": backup_title,
            "Backup Person LinkedIn": backup_linkedin,
            "Employee LinkedIn 1": employee_urls[0] if len(employee_urls) > 0 else "",
            "Employee LinkedIn 2": employee_urls[1] if len(employee_urls) > 1 else "",
            "Employee LinkedIn 3": employee_urls[2] if len(employee_urls) > 2 else "",
            "Company LinkedIn": company_linkedin,
            "Company People Search": people_search,
            "Notes": str(lead["notes"] or "").strip(),
            "Tags": str(lead["tags"] or "").strip(),
        })
        if len(fallback_rows) >= MAX_LINKEDIN_FALLBACK_ROWS:
            break

    fallback_rows.sort(
        key=lambda row: (
            -int(row["Heat Score"] or 0),
            -_priority_rank(row["Priority"]),
            row["Org Name"],
        )
    )
    return fallback_rows


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    rows = _apply_mx_gate(_best_rows())
    linkedin_rows = _build_linkedin_fallback_rows(rows)
    _write_csv(STRICT_EXPORT, rows)
    _write_csv(STRICT_TOP_50, rows[:50])
    _write_csv(LINKEDIN_FALLBACK_EXPORT, linkedin_rows)
    _write_csv(LINKEDIN_FALLBACK_TOP_50, linkedin_rows[:50])

    tiers = Counter(row["Tier"] or "Unknown" for row in rows)
    print(f"Wrote {len(rows)} strict person-email leads to {STRICT_EXPORT}")
    print(f"Wrote {min(50, len(rows))} top leads to {STRICT_TOP_50}")
    print(f"Tier mix: {dict(tiers)}")
    print(f"Wrote {len(linkedin_rows)} LinkedIn fallback leads to {LINKEDIN_FALLBACK_EXPORT}")
    print(f"Wrote {min(50, len(linkedin_rows))} top LinkedIn fallback leads to {LINKEDIN_FALLBACK_TOP_50}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())