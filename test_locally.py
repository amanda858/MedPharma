#!/usr/bin/env python3
"""Local test of email generation workflow."""

import asyncio
import sys
import os

# Setup
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

async def test_email_system():
    """Test the complete email generation workflow."""
    print("Testing email generation system...")

    try:
        # Test imports
        from app.enrichment import enrich_lead
        from app.email_finder import find_emails_for_lab
        from app.database import init_db, save_enrichment

        print("OK: Imports successful")

        # Initialize database
        init_db()
        print("OK: Database initialized")

        # Test enrichment (this will fail with fake NPI but shows the logic)
        print("Testing enrichment...")
        enrichment = await enrich_lead(
            npi="1234567890",
            org_name="Test Medical Lab",
            state="CA",
            city="Los Angeles"
        )
        print(f"Enrichment result: {enrichment}")

        # Test email generation with known names
        print("Testing email generation...")
        result = await find_emails_for_lab(
            "Test Medical Laboratory",
            first_name="John",
            last_name="Smith"
        )
        print(f"Email generation result: {result}")

        emails = result.get('emails', [])
        if emails:
            print(f"OK: Generated {len(emails)} emails:")
            for email in emails[:3]:  # Show first 3
                print(f"   - {email['email']} ({email['first_name']} {email['last_name']})")
        else:
            print("No emails generated")

        print("Test completed")

    except Exception as e:
        print(f"ERROR: Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_email_system())