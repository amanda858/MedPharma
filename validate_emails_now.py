#!/usr/bin/env python3
"""Validate existing emails in the database using free MX + SMTP verification.

No API key required — uses the built-in email_verifier module.
"""

import asyncio
import os
import sys
from typing import Dict

sys.path.insert(0, os.path.dirname(__file__))

from app.database import get_db
from app.email_verifier import verify_batch


async def validate_existing_emails(batch_size: int = 50, delay: float = 1.0) -> Dict:
    """
    Validate existing emails in database using free MX + SMTP verification.
    Removes role/generic addresses (info@, contact@, etc.) automatically.
    """
    db = get_db()

    emails_to_validate = db.execute("""
        SELECT npi, email, confidence, source
        FROM lead_emails
        WHERE confidence < 90 OR source NOT IN ('pattern_smtp_verified', 'mx_verified_pattern')
        ORDER BY confidence ASC
        LIMIT ?
    """, (batch_size,)).fetchall()

    if not emails_to_validate:
        db.close()
        return {"message": "No emails found that need validation"}

    print(f"Validating {len(emails_to_validate)} emails (free MX+SMTP — no API key needed)...")

    email_list = [row['email'] for row in emails_to_validate]
    results = await verify_batch(email_list, do_smtp=True, concurrency=4)
    result_map = {r['email']: r for r in results}

    validated = 0
    improved = 0
    failed = 0
    removed = 0

    for row in emails_to_validate:
        email = row['email']
        current_confidence = row['confidence']
        v = result_map.get(email, {})
        verdict = v.get('verdict', 'unknown')
        score = int(v.get('score', 0))
        is_role = bool(v.get('is_role', False))

        if is_role:
            # Always remove role/generic addresses — they are never actionable
            db.execute("DELETE FROM lead_emails WHERE email = ?", (email,))
            removed += 1
            print(f"❌ {email}: removed (role/generic address)")

        elif verdict == 'deliverable':
            new_confidence = min(90, max(current_confidence, score))
            db.execute("""
                UPDATE lead_emails
                SET confidence = ?, source = 'pattern_smtp_verified'
                WHERE email = ?
            """, (new_confidence, email))
            validated += 1
            if new_confidence > current_confidence:
                improved += 1
            print(f"✅ {email}: {current_confidence} → {new_confidence}")

        elif verdict in ('catch-all', 'risky'):
            new_confidence = min(current_confidence, 55)
            db.execute("""
                UPDATE lead_emails
                SET confidence = ?, source = 'mx_verified_pattern'
                WHERE email = ?
            """, (new_confidence, email))
            failed += 1
            print(f"⚠️  {email}: {verdict}, confidence set to {new_confidence}")

        elif verdict == 'undeliverable':
            if current_confidence < 50:
                db.execute("DELETE FROM lead_emails WHERE email = ?", (email,))
                removed += 1
                print(f"❌ {email}: removed (undeliverable)")
            else:
                failed += 1
                print(f"❓ {email}: kept despite failed verification (high confidence)")

        else:
            failed += 1
            print(f"❓ {email}: verdict={verdict}, no change")

        await asyncio.sleep(delay)

    db.commit()
    db.close()

    return {
        "total_processed": len(emails_to_validate),
        "validated": validated,
        "improved": improved,
        "failed": failed,
        "removed": removed,
        "message": f"Validation complete. {validated} verified, {improved} improved, {removed} removed.",
    }


async def check_email_quality_stats() -> Dict:
    """Get statistics on email quality in the database."""
    db = get_db()
    total_emails = db.execute("SELECT COUNT(*) FROM lead_emails").fetchone()[0]
    confidence_ranges = db.execute("""
        SELECT
            COUNT(CASE WHEN confidence >= 90 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN confidence >= 70 AND confidence < 90 THEN 1 END) as medium_confidence,
            COUNT(CASE WHEN confidence >= 50 AND confidence < 70 THEN 1 END) as low_confidence,
            COUNT(CASE WHEN confidence < 50 THEN 1 END) as very_low_confidence
        FROM lead_emails
    """).fetchone()
    source_stats = db.execute("""
        SELECT source, COUNT(*) as count
        FROM lead_emails
        GROUP BY source
        ORDER BY count DESC
    """).fetchall()
    db.close()
    return {
        "total_emails": total_emails,
        "confidence_distribution": {
            "high_90+": confidence_ranges['high_confidence'],
            "medium_70-89": confidence_ranges['medium_confidence'],
            "low_50-69": confidence_ranges['low_confidence'],
            "very_low_<50": confidence_ranges['very_low_confidence'],
        },
        "source_distribution": {row['source']: row['count'] for row in source_stats},
    }


async def main():
    print("Email Validation Tool (free MX+SMTP — no API key needed)")
    print("=" * 55)

    print("Current email quality statistics:")
    stats = await check_email_quality_stats()
    print(f"Total emails: {stats['total_emails']}")
    conf_dist = stats['confidence_distribution']
    print(f"High confidence (90+): {conf_dist['high_90+']}")
    print(f"Medium confidence (70-89): {conf_dist['medium_70-89']}")
    print(f"Low confidence (50-69): {conf_dist['low_50-69']}")
    print(f"Very low confidence (<50): {conf_dist['very_low_<50']}")

    print("\nSource distribution:")
    for source, count in stats['source_distribution'].items():
        print(f"  {source}: {count}")

    print("\n" + "=" * 55)
    print("Starting email validation...")
    result = await validate_existing_emails(batch_size=50, delay=1.0)

    print(f"\nValidation Results:")
    print(f"✅ Processed: {result['total_processed']} emails")
    print(f"✅ Verified:  {result['validated']} emails")
    print(f"📈 Improved:  {result['improved']} emails")
    print(f"❌ Failed:    {result['failed']} emails")
    print(f"🗑️  Removed:   {result['removed']} emails")
    print(f"\n{result['message']}")

    print("\nFinal email quality statistics:")
    final_stats = await check_email_quality_stats()
    final_conf = final_stats['confidence_distribution']
    print(f"High confidence (90+): {final_conf['high_90+']} ({final_conf['high_90+'] - conf_dist['high_90+']:+d})")
    print(f"Medium confidence (70-89): {final_conf['medium_70-89']}")
    print(f"Low confidence (50-69): {final_conf['low_50-69']}")
    print(f"Very low confidence (<50): {final_conf['very_low_<50']} ({final_conf['very_low_<50'] - conf_dist['very_low_<50']:+d})")


if __name__ == "__main__":
    asyncio.run(main())
