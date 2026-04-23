"""One-time backfill for missing requested service intent on saved leads.

Usage:
  python3 backfill_requested_services.py           # dry run
  python3 backfill_requested_services.py --apply   # write updates

The script is conservative:
- It only updates leads missing requested_services in tags.
- It prefers explicit note-based intent.
- It falls back to enrichment-inferred services only when no explicit clues exist.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime


DB_PATH = os.getenv("DB_PATH", "data/leads.db")

SERVICE_KEYWORDS = {
    "billing": [
        "revenue cycle", "rcm", "billing", "claims", "ar", "accounts receivable", "denials", "collections",
    ],
    "credentialing": [
        "credentialing", "enrollment", "provider enrollment", "payer enrollment", "caqh", "pecos",
    ],
    "compliance_workflow": [
        "compliance", "workflow", "operations", "audit", "clia", "regulatory", "turnaround", "backlog",
    ],
}

ENRICHMENT_LABEL_TO_TOKEN = {
    "Billing Services": "billing",
    "Payor Contracting": "credentialing",
    "Workflow Support": "compliance_workflow",
}


@dataclass
class BackfillDecision:
    tokens: list[str]
    source: str


def _has_requested_token(tags: str) -> bool:
    if not tags:
        return False
    return any(part.strip().lower().startswith("requested_services=") for part in tags.split(","))


def _extract_from_text(text: str) -> list[str]:
    text_norm = (text or "").lower()
    matches: list[str] = []
    for token, keywords in SERVICE_KEYWORDS.items():
        if any(kw in text_norm for kw in keywords):
            matches.append(token)
    return matches


def _extract_from_enrichment(raw_services_needed: str) -> list[str]:
    if not raw_services_needed:
        return []
    try:
        parsed = json.loads(raw_services_needed)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []

    out: list[str] = []
    for label in parsed:
        token = ENRICHMENT_LABEL_TO_TOKEN.get(str(label), "")
        if token and token not in out:
            out.append(token)
    return out


def _decide_requested_services(notes: str, raw_services_needed: str) -> BackfillDecision | None:
    from_notes = _extract_from_text(notes)
    if from_notes:
        return BackfillDecision(tokens=from_notes, source="notes")

    from_enrichment = _extract_from_enrichment(raw_services_needed)
    if from_enrichment:
        return BackfillDecision(tokens=from_enrichment, source="enrichment_inferred")

    return None


def _append_requested_tag(existing_tags: str, tokens: list[str], source: str) -> str:
    base = [p.strip() for p in (existing_tags or "").split(",") if p.strip()]
    requested_value = "|".join(tokens)
    base.append(f"requested_services={requested_value}")
    base.append(f"requested_services_source={source}")
    return ",".join(base)


def run_backfill(apply_changes: bool) -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT sl.id, sl.npi, sl.organization_name, sl.notes, sl.tags,
               COALESCE(le.services_needed, '[]') AS services_needed
        FROM saved_leads sl
        LEFT JOIN lead_enrichment le ON sl.npi = le.npi
        ORDER BY sl.id ASC
        """
    )
    rows = cur.fetchall()

    scanned = 0
    already_tagged = 0
    updated = 0
    inferred_count = 0
    skipped = 0

    for row in rows:
        scanned += 1
        current_tags = row["tags"] or ""

        if _has_requested_token(current_tags):
            already_tagged += 1
            continue

        decision = _decide_requested_services(row["notes"] or "", row["services_needed"] or "[]")
        if not decision or not decision.tokens:
            skipped += 1
            continue

        if decision.source != "notes":
            inferred_count += 1

        new_tags = _append_requested_tag(current_tags, decision.tokens, decision.source)
        if apply_changes:
            cur.execute(
                "UPDATE saved_leads SET tags = ?, updated_at = ? WHERE id = ?",
                (new_tags, datetime.now().isoformat(), row["id"]),
            )
        updated += 1

    if apply_changes:
        conn.commit()
    conn.close()

    mode = "APPLY" if apply_changes else "DRY RUN"
    print(f"[{mode}] Backfill requested_services complete")
    print(f"- DB: {DB_PATH}")
    print(f"- Scanned: {scanned}")
    print(f"- Already tagged: {already_tagged}")
    print(f"- Updated: {updated}")
    print(f"- Updated from inferred enrichment: {inferred_count}")
    print(f"- Skipped (insufficient evidence): {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill requested_services tags on saved leads")
    parser.add_argument("--apply", action="store_true", help="Persist changes (default is dry run)")
    args = parser.parse_args()

    run_backfill(apply_changes=args.apply)
