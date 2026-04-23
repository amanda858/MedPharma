#!/usr/bin/env python3
"""Test script to check enrichment data in database."""

import sys
import os
import json

# Add the project root to the path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.database import get_db, get_enrichment

def check_enrichment():
    """Check enrichment data in database."""
    db = get_db()
    
    # Check how many leads have enrichment
    enriched_count = db.execute("SELECT COUNT(*) FROM lead_enrichment").fetchone()[0]
    print(f"Total enriched leads: {enriched_count}")
    
    # Check a few examples
    enrichments = db.execute("SELECT npi, organization_name, authorized_official FROM lead_enrichment LIMIT 5").fetchall()
    for row in enrichments:
        npi = row['npi']
        org = row['organization_name']
        auth_json = row['authorized_official']
        try:
            auth = json.loads(auth_json) if auth_json else {}
            first = auth.get('first_name', '')
            last = auth.get('last_name', '')
            print(f"NPI {npi} ({org}): {first} {last}")
        except json.JSONDecodeError:
            print(f"NPI {npi} ({org}): invalid JSON")
    
    # Check emails
    email_count = db.execute("SELECT COUNT(*) FROM lead_emails").fetchone()[0]
    print(f"Total emails: {email_count}")
    
    # Check some emails
    emails = db.execute("SELECT npi, email, first_name, last_name FROM lead_emails LIMIT 5").fetchall()
    for row in emails:
        print(f"Email: {row['email']} for {row['first_name']} {row['last_name']} (NPI: {row['npi']})")
    
    db.close()

if __name__ == "__main__":
    check_enrichment()