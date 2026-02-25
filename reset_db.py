"""Run once to wipe and rebuild the database with correct seed data."""
import os

db_path = os.getenv("DB_PATH", "data/leads.db")
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Deleted {db_path}")

from app.database import init_db
from app.client_db import init_client_hub_db

init_db()
init_client_hub_db()
print("Database rebuilt. Clients seeded: admin / Luminary (OMT/MHP) / TruPath / KinderCare")
