#!/usr/bin/env python3
"""Validate existing emails in the database using Hunter.io API."""

import asyncio
import os
import sys
from typing import Dict

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from app.database import get_db
from app.email_finder import hunter_verify_email
from app.config import HUNTER_API_KEY


async def validate_existing_emails(batch_size: int = 50, delay: float = 1.0) -> Dict:
    """
    Validate existing emails in database using Hunter.io verification.

    Args:
        batch_size: Number of emails to validate per batch
        delay: Delay between API calls to avoid rate limits

    Returns:
        Dict with validation statistics
    """
    if not HUNTER_API_KEY:
        return {"error": "HUNTER_API_KEY not set. Cannot validate emails."}

    db = get_db()

    # Get all emails that need validation
    emails_to_validate = db.execute("""
        SELECT npi, email, confidence, source
        FROM lead_emails
        WHERE confidence < 90 OR source NOT LIKE 'hunter.io%'
        ORDER BY confidence ASC
        LIMIT ?
    """, (batch_size,)).fetchall()

    if not emails_to_validate:
        db.close()
        return {"message": "No emails found that need validation"}

    print(f"Validating {len(emails_to_validate)} emails...")

    validated = 0
    improved = 0
    failed = 0
    removed = 0

    for row in emails_to_validate:
        email = row['email']
        current_confidence = row['confidence']

        try:
            # Verify email using Hunter.io
            verification = await hunter_verify_email(email, HUNTER_API_KEY)

            if verification.get('is_valid') and verification.get('score', 0) >= 70:
                # Email is good - update confidence
                new_confidence = min(95, max(current_confidence, verification['score']))
                db.execute("""
                    UPDATE lead_emails
                    SET confidence = ?, source = 'hunter.io/verified'
                    WHERE email = ?
                """, (new_confidence, email))
                validated += 1
                if new_confidence > current_confidence:
                    improved += 1
                print(f"✅ {email}: {current_confidence} → {new_confidence}")

            elif verification.get('status') in ('accept_all',):
                # Accept-all is risky for outbound campaigns: keep but downgrade.
                new_confidence = min(current_confidence, 55)
                db.execute("""
                    UPDATE lead_emails
                    SET confidence = ?, source = 'hunter.io/accept_all_risky'
                    WHERE email = ?
                """, (new_confidence, email))
                failed += 1
                print(f"⚠️  {email}: accept-all (risky), confidence set to {new_confidence}")

            else:
                # Email failed verification - remove if confidence was low
                if current_confidence < 50:
                    db.execute("DELETE FROM lead_emails WHERE email = ?", (email,))
                    removed += 1
                    print(f"❌ {email}: removed (failed verification)")
                else:
                    failed += 1
                    print(f"❓ {email}: kept despite failed verification (high confidence)")

            db.commit()

        except Exception as e:
            print(f"Error validating {email}: {e}")
            failed += 1

        # Rate limiting
        await asyncio.sleep(delay)

    db.close()

    return {
        "total_processed": len(emails_to_validate),
        "validated": validated,
        "improved": improved,
        "failed": failed,
        "removed": removed,
        "message": f"Validation complete. {validated} emails validated, {improved} improved, {removed} removed."
    }


async def check_email_quality_stats() -> Dict:
    """Get statistics on email quality in the database."""
    db = get_db()

    # Overall stats
    total_emails = db.execute("SELECT COUNT(*) FROM lead_emails").fetchone()[0]

    # Confidence distribution
    confidence_ranges = db.execute("""
        SELECT
            COUNT(CASE WHEN confidence >= 90 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN confidence >= 70 AND confidence < 90 THEN 1 END) as medium_confidence,
            COUNT(CASE WHEN confidence >= 50 AND confidence < 70 THEN 1 END) as low_confidence,
            COUNT(CASE WHEN confidence < 50 THEN 1 END) as very_low_confidence
        FROM lead_emails
    """).fetchone()

    # Source distribution
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
            "very_low_<50": confidence_ranges['very_low_confidence']
        },
        "source_distribution": {row['source']: row['count'] for row in source_stats}
    }


async def main():
    """Main validation function."""
    print("Email Validation Tool")
    print("=" * 50)

    # Check current quality stats
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

    print("\n" + "=" * 50)

    # Validate emails
    if not HUNTER_API_KEY:
        print("❌ HUNTER_API_KEY not set. Skipping validation.")
        return

    print("Starting email validation...")
    result = await validate_existing_emails(batch_size=20, delay=1.5)

    if "error" in result:
        print(f"❌ Error: {result['error']}")
        return

    print(f"\nValidation Results:")
    print(f"✅ Processed: {result['total_processed']} emails")
    print(f"✅ Validated: {result['validated']} emails")
    print(f"📈 Improved: {result['improved']} emails")
    print(f"❌ Failed: {result['failed']} emails")
    print(f"🗑️  Removed: {result['removed']} emails")
    print(f"\n{result['message']}")

    # Final stats
    print("\nFinal email quality statistics:")
    final_stats = await check_email_quality_stats()
    final_conf = final_stats['confidence_distribution']
    print(f"High confidence (90+): {final_conf['high_90+']} (+{final_conf['high_90+'] - conf_dist['high_90+']})")
    print(f"Medium confidence (70-89): {final_conf['medium_70-89']}")
    print(f"Low confidence (50-69): {final_conf['low_50-69']}")
    print(f"Very low confidence (<50): {final_conf['very_low_<50']} ({final_conf['very_low_<50'] - conf_dist['very_low_<50']})")


if __name__ == "__main__":
    asyncio.run(main())