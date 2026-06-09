#!/usr/bin/env python3
"""Run the client hub locally for testing."""

import os
import sys

# Set environment for local testing
os.environ['SERVICE'] = 'hub'
os.environ['DB_PATH'] = 'data/leads.db'

# Add project to path
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if __name__ == "__main__":
    from app.hub_app import app
    import uvicorn

    print("Starting client hub locally...")
    print("Open: http://localhost:8000/hub")

    uvicorn.run(app, host="0.0.0.0", port=8000)