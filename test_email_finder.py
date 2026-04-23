#!/usr/bin/env python3
"""Test email finding for a specific lead."""

import asyncio
import sys
import os

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.email_finder import find_emails_for_lab

async def test_email_finding():
    # Test with a known lead
    result = await find_emails_for_lab(
        'LEMEDIX LAB LLC',
        first_name='John',
        last_name='Smith'
    )

    print("Email finding result:")
    print(f"Organization: {result['org_name']}")
    print(f"Domain found: {result['live_domain']}")
    print(f"Emails found: {len(result.get('emails', []))}")

    for email in result.get('emails', []):
        print(f"  - {email['email']} (confidence: {email['confidence']}%, source: {email['source']})")

    if result.get('error'):
        print(f"Error: {result['error']}")

if __name__ == "__main__":
    asyncio.run(test_email_finding())