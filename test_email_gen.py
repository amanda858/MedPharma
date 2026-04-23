#!/usr/bin/env python3
"""Quick test of email generation functionality."""

import asyncio
import sys
import os

# Add project root to path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

async def test_email_generation():
    """Test the email generation logic."""
    try:
        from app.enrichment import enrich_lead
        from app.email_finder import find_emails_for_lab

        print("Testing enrichment...")
        # Test with a known NPI
        enrichment = await enrich_lead(
            npi="1234567890",  # This will likely fail but test the logic
            org_name="Test Lab",
            state="CA",
            city="Los Angeles"
        )
        print(f"Enrichment result: {enrichment}")

        print("Testing email generation...")
        # Test email generation with dummy data
        result = await find_emails_for_lab(
            "Test Laboratory Inc",
            first_name="John",
            last_name="Smith"
        )
        print(f"Email generation result: {result}")

        print("✅ Basic functionality test completed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_email_generation())