#!/usr/bin/env python3
"""Verify every email in a scrubbed CSV using the in-house SMTP verifier.

Usage:
    python3 verify_csv.py <input.csv> [output.csv]

Reads Email 1..5 columns, runs MX + SMTP RCPT verification on each, writes a
new CSV with these added columns:
    Email N Verdict   (deliverable | catch-all | undeliverable | risky | unknown)
    Email N MX        (true/false)
    Email N SMTP      (ok | rejected | tempfail | error | none)
And re-orders Email 1..5 so the most deliverable address is Email 1.

Run this from a network that allows outbound port 25. Render and most cloud
hosts block port 25, so this is intended for a dev container, laptop, or any
machine with unrestricted outbound network access.
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
from pathlib import Path

# Make the app package importable
sys.path.insert(0, str(Path(__file__).parent.resolve()))

from app.email_verifier import verify_batch


EMAIL_COLS = [f"Email {i}" for i in range(1, 6)]
SCORE_COLS = [f"Email {i} Score" for i in range(1, 6)]


def _verdict_rank(v: str) -> int:
    return {
        "deliverable": 100,
        "catch-all": 55,
        "risky": 40,
        "unknown": 30,
        "undeliverable": 0,
    }.get(v or "unknown", 30)


async def verify_row(row: dict) -> dict:
    emails = [row.get(c, "").strip() for c in EMAIL_COLS]
    emails = [e for e in emails if e]
    if not emails:
        return row

    results = await verify_batch(emails, do_smtp=True, concurrency=4)
    by_email = {r["email"]: r for r in results}

    # Re-score & reorder: rank by SMTP verdict, then existing score
    reranked: list[tuple[int, str, dict]] = []
    for i, e in enumerate(emails):
        v = by_email.get(e, {})
        verdict = v.get("verdict", "unknown")
        if verdict == "undeliverable":
            continue  # drop entirely
        rank_score = _verdict_rank(verdict)
        try:
            existing = int(row.get(SCORE_COLS[i], "") or 0)
        except (ValueError, TypeError):
            existing = 0
        combined = max(existing, rank_score)
        reranked.append((combined, e, v))

    reranked.sort(key=lambda x: -x[0])

    # Write back into Email 1..5
    for i in range(5):
        if i < len(reranked):
            sc, e, v = reranked[i]
            row[EMAIL_COLS[i]] = e
            row[SCORE_COLS[i]] = sc
            row[f"Email {i+1} Verdict"] = v.get("verdict", "")
            row[f"Email {i+1} MX"] = "true" if v.get("mx_found") else "false"
            row[f"Email {i+1} SMTP"] = v.get("smtp_result") or "none"
        else:
            row[EMAIL_COLS[i]] = ""
            row[SCORE_COLS[i]] = ""
            row[f"Email {i+1} Verdict"] = ""
            row[f"Email {i+1} MX"] = ""
            row[f"Email {i+1} SMTP"] = ""
    return row


async def main(in_path: str, out_path: str) -> None:
    with open(in_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        in_fields = reader.fieldnames or []

    print(f"[verify_csv] {len(rows)} rows from {in_path}")

    out_fields = list(in_fields)
    for i in range(1, 6):
        for col in (f"Email {i} Verdict", f"Email {i} MX", f"Email {i} SMTP"):
            if col not in out_fields:
                out_fields.append(col)

    new_rows: list[dict] = []
    for idx, row in enumerate(rows, 1):
        new_row = await verify_row(row)
        new_rows.append(new_row)
        deliv = sum(1 for i in range(1, 6) if new_row.get(f"Email {i} Verdict") == "deliverable")
        rejected = sum(1 for i in range(1, 6) if new_row.get(f"Email {i} Verdict") == "")
        print(f"  [{idx}/{len(rows)}] {row.get('Org Name', '?')[:40]:40s}  deliverable={deliv}  rejected={rejected}")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        w.writeheader()
        for r in new_rows:
            w.writerow({k: r.get(k, "") for k in out_fields})

    print(f"[verify_csv] wrote {out_path}")
    # Summary
    total_deliv = sum(
        1 for r in new_rows for i in range(1, 6)
        if r.get(f"Email {i} Verdict") == "deliverable"
    )
    total_catchall = sum(
        1 for r in new_rows for i in range(1, 6)
        if r.get(f"Email {i} Verdict") == "catch-all"
    )
    print(f"[verify_csv] summary: {total_deliv} deliverable, {total_catchall} catch-all across {len(new_rows)} rows")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 verify_csv.py <input.csv> [output.csv]", file=sys.stderr)
        sys.exit(2)
    in_path = sys.argv[1]
    if not os.path.exists(in_path):
        print(f"Not found: {in_path}", file=sys.stderr)
        sys.exit(2)
    out_path = sys.argv[2] if len(sys.argv) >= 3 else in_path.replace(".csv", "_verified.csv")
    asyncio.run(main(in_path, out_path))
