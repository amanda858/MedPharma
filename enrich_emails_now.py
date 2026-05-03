#!/usr/bin/env python3
"""Backfill lead emails for saved leads that currently have none."""

import asyncio
import os
import sys

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.database import get_db
from app.email_finder import find_emails_for_lab

def _print_hunter_mode() -> None:
    # Hunter is optional; the finder can still work with scraping fallbacks.
    api_key = os.getenv("HUNTER_API_KEY")
    if api_key:
        print("HUNTER_API_KEY found, Hunter-based validation available")
    else:
        print("No HUNTER_API_KEY, using scraping-only mode")

async def main():
    _print_hunter_mode()
    db = get_db()
    leads = db.execute("SELECT npi, organization_name, city, state FROM saved_leads").fetchall()
    print(f'Found {len(leads)} leads in database')

    updated = 0
    for lead in leads:
        npi = lead['npi']
        org_name = lead['organization_name']
        # Check if already has emails
        existing_emails = db.execute("SELECT COUNT(*) FROM lead_emails WHERE npi = ?", (npi,)).fetchone()[0]
        if existing_emails == 0:
            try:
                emails_result = await find_emails_for_lab(org_name)
                emails = emails_result.get('emails', [])
                if emails:
                    for email in emails:
                        db.execute("""
                            INSERT OR IGNORE INTO lead_emails 
                            (npi, email, first_name, last_name, position, is_decision_maker, confidence, source, domain)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            npi,
                            email['email'],
                            email.get('first_name', ''),
                            email.get('last_name', ''),
                            email.get('position', ''),
                            1 if email.get('is_decision_maker') else 0,
                            email.get('confidence', 0),
                            email.get('source', 'scraped'),
                            email.get('domain', '')
                        ))
                    db.commit()
                    updated += 1
                    print(f'Updated {org_name} with {len(emails)} emails')
            except Exception as e:
                print(f'Error finding emails for {org_name}: {e}')

    print(f'Updated {updated} leads with emails')
    db.close()

if __name__ == "__main__":
    asyncio.run(main())