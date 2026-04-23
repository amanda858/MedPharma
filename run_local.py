#!/usr/bin/env python3
"""Run the leads app locally for testing."""

import os
import sys

# Set environment for local testing
os.environ['SERVICE'] = 'leads'  # Run leads app directly
os.environ['DB_PATH'] = 'data/leads.db'  # Local database

# Add project to path
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if __name__ == "__main__":
    from app.leads_app import app
    import uvicorn

    print("Starting leads app locally...")
    print("Open: http://localhost:8000")
    print("Test URLs:")
    print("   Enrich: http://localhost:8000/api/admin/enrich-leads")
    print("   Emails: http://localhost:8000/api/admin/enrich-emails")
    print("   Export: http://localhost:8000/api/export/emails/csv")

    uvicorn.run(app, host="0.0.0.0", port=8000)