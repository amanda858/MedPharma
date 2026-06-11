#!/usr/bin/env python3
"""Executable local smoke tests for the supported hub runtime."""

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path


def _bootstrap_temp_env(tmpdir: Path):
    os.environ["DB_PATH"] = str(tmpdir / "hub.db")
    if "app.config" in sys.modules:
        importlib.reload(sys.modules["app.config"])
    client_db = importlib.import_module("app.client_db")
    client_db = importlib.reload(client_db)
    client_db._CLIENTS_SEED_PATH = str(tmpdir / "clients_seed.json")
    Path(client_db._CLIENTS_SEED_PATH).write_text("[]\n", encoding="utf-8")
    hub_app = importlib.import_module("app.hub_app")
    hub_app = importlib.reload(hub_app)
    return client_db, hub_app


def _assert(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    with tempfile.TemporaryDirectory(prefix="cvopro-hub-smoke-") as tmp:
        tmpdir = Path(tmp)
        client_db, hub_app = _bootstrap_temp_env(tmpdir)

        from fastapi.testclient import TestClient

        with TestClient(hub_app.app) as client:
            ready = client.get("/readyz")
            _assert(ready.status_code == 200, f"/readyz expected 200, got {ready.status_code}: {ready.text}")

            leads_removed = client.get("/admin/leads/api/leads/stats")
            _assert(leads_removed.status_code == 410, f"removed leads surface expected 410, got {leads_removed.status_code}")

            admin_login = client.post("/hub/api/login", json={"username": "admin", "password": "admin123"})
            _assert(admin_login.status_code == 200, f"admin login failed: {admin_login.status_code} {admin_login.text}")

            me = client.get("/hub/api/me")
            _assert(me.status_code == 200, f"/hub/api/me expected 200, got {me.status_code}")

            created_client_id = client_db.create_client({
                "company": "Smoke Test Client",
                "contact_name": "Smoke Owner",
                "email": "smoke@example.com",
                "phone": "555-0100",
                "role": "client",
                "service_type": "rcm",
                "notes": "created by smoke test",
            })
            created_seed = client_db._load_clients_seed()
            _assert(any(entry.get("email") == "smoke@example.com" for entry in created_seed), "created client missing from seed")

            client_db.update_client(created_client_id, {"notes": "updated by smoke test"})
            updated_snapshot = next(item for item in client_db.list_clients() if item["id"] == created_client_id)
            _assert(updated_snapshot["company"] == "Smoke Test Client", "updated client snapshot mismatch")

            staff_id = client_db.create_client({
                "username": "staffsmoke",
                "password": "staffpass12345",
                "company": "MedPharma SC",
                "contact_name": "Staff Smoke",
                "email": "staffsmoke@example.com",
                "phone": "555-0200",
                "role": "staff",
                "service_type": "operations",
            })
            _assert(staff_id > 0, "staff user creation failed")

            client.post("/hub/api/logout")
            staff_login = client.post("/hub/api/login", json={"username": "staffsmoke", "password": "staffpass12345"})
            _assert(staff_login.status_code == 200, f"staff login failed: {staff_login.status_code} {staff_login.text}")

            relink = client.post("/hub/api/admin/production/relink-kindercare", json={})
            _assert(relink.status_code == 403, f"staff relink expected 403, got {relink.status_code}: {relink.text}")

            client.post("/hub/api/logout")
            after_logout = client.get("/hub/api/me")
            _assert(after_logout.status_code == 401, f"post-logout me expected 401, got {after_logout.status_code}")

            client_db.delete_client(created_client_id)
            client_db.delete_client(staff_id)
            remaining_seed = client_db._load_clients_seed()
            _assert(not any(entry.get("email") == "smoke@example.com" for entry in remaining_seed), "deleted client remained in seed")

            print(json.dumps({
                "readyz": ready.status_code,
                "removed_leads": leads_removed.status_code,
                "created_client_id": created_client_id,
                "staff_forbidden_relink": relink.status_code,
                "remaining_seed_count": len(remaining_seed),
            }, indent=2))
            print("hub_smoke_ok")


if __name__ == "__main__":
    main()