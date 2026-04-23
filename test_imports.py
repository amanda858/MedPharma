#!/usr/bin/env python3
"""Test script to check if modules can be imported."""

import sys
import os

# Add project to path
ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, ROOT_DIR)

try:
    from app.leads_app import app as leads_app
    print("✅ leads_app imported successfully")
except Exception as e:
    print(f"❌ Error importing leads_app: {e}")

try:
    from app.hub_app import app as hub_app
    print("✅ hub_app imported successfully")
except Exception as e:
    print(f"❌ Error importing hub_app: {e}")

try:
    import multiprocessing
    print("✅ multiprocessing imported successfully")
except Exception as e:
    print(f"❌ Error importing multiprocessing: {e}")

try:
    import uvicorn
    print("✅ uvicorn imported successfully")
except Exception as e:
    print(f"❌ Error importing uvicorn: {e}")