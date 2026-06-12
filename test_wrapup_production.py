import importlib
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def hub_env(tmp_path):
    os.environ["DB_PATH"] = str(tmp_path / "hub.db")
    for mod in ("app.config", "app.client_db", "app.client_routes", "app.hub_app"):
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    client_db = importlib.import_module("app.client_db")
    client_db = importlib.reload(client_db)
    client_db._CLIENTS_SEED_PATH = str(tmp_path / "clients_seed.json")
    Path(client_db._CLIENTS_SEED_PATH).write_text("[]\n", encoding="utf-8")
    hub_app = importlib.import_module("app.hub_app")
    hub_app = importlib.reload(hub_app)
    return client_db, hub_app


def _login(client: TestClient, username: str, password: str):
    r = client.post("/hub/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text


def test_production_delete_ownership_enforced(hub_env):
    client_db, hub_app = hub_env
    with TestClient(hub_app.app) as client:
        owner_id = client_db.create_client({
            "username": "owner1",
            "password": "ownerpass123",
            "company": "Owner One",
            "contact_name": "Owner One",
            "email": "owner1@example.com",
            "phone": "555-1000",
            "role": "client",
        })
        other_id = client_db.create_client({
            "username": "other1",
            "password": "otherpass123",
            "company": "Other One",
            "contact_name": "Other One",
            "email": "other1@example.com",
            "phone": "555-1001",
            "role": "client",
        })

        _login(client, "owner1", "ownerpass123")
        create = client.post("/hub/api/production", json={
            "client_id": owner_id,
            "work_date": "2026-06-12",
            "category": "Reporting",
            "task_description": "Owner work",
            "quantity": 2,
            "time_spent": 1.5,
            "notes": "",
        })
        assert create.status_code == 200, create.text
        log_id = create.json()["id"]

        client.post("/hub/api/logout")
        _login(client, "other1", "otherpass123")
        denied = client.delete(f"/hub/api/production/{log_id}")
        assert denied.status_code == 404, denied.text

        client.post("/hub/api/logout")
        _login(client, "admin", "admin123")
        allowed = client.delete(f"/hub/api/production/{log_id}")
        assert allowed.status_code == 200, allowed.text

        client_db.delete_client(owner_id)
        client_db.delete_client(other_id)


def test_admin_routes_exist_for_snapshot_and_notifications(hub_env, monkeypatch):
    _, hub_app = hub_env
    routes = importlib.import_module("app.client_routes")
    monkeypatch.setattr(routes, "get_notification_debug", lambda: {"ok": True, "debug": "stub"})
    monkeypatch.setattr(routes, "send_daily_account_summary", lambda: None)

    with TestClient(hub_app.app) as client:
        _login(client, "admin", "admin123")

        snapshot = client.get("/hub/api/production/snapshot")
        assert snapshot.status_code == 200, snapshot.text
        assert "user_stats" in snapshot.json()

        debug = client.get("/hub/api/notifications/debug")
        assert debug.status_code == 200, debug.text
        assert debug.json().get("ok") is True

        daily = client.post("/hub/api/notifications/daily-report")
        assert daily.status_code == 200, daily.text
        assert daily.json().get("ok") is True


def test_admin_production_is_all_accounts(hub_env):
    client_db, hub_app = hub_env
    with TestClient(hub_app.app) as client:
        owner_id = client_db.create_client({
            "username": "scope_owner",
            "password": "scopepass123",
            "company": "Scope Owner",
            "contact_name": "Scope Owner",
            "email": "scope_owner@example.com",
            "phone": "555-1100",
            "role": "client",
        })
        other_id = client_db.create_client({
            "username": "scope_other",
            "password": "scopepass456",
            "company": "Scope Other",
            "contact_name": "Scope Other",
            "email": "scope_other@example.com",
            "phone": "555-1101",
            "role": "client",
        })

        client_db.add_production_log({
            "client_id": owner_id,
            "work_date": "2026-06-12",
            "username": "scope_owner",
            "category": "Billing",
            "task_description": "Owner task",
            "quantity": 1,
            "time_spent": 1.0,
            "notes": "",
        })
        client_db.add_production_log({
            "client_id": other_id,
            "work_date": "2026-06-12",
            "username": "scope_other",
            "category": "Claims",
            "task_description": "Other task",
            "quantity": 1,
            "time_spent": 1.0,
            "notes": "",
        })

        _login(client, "admin", "admin123")
        response = client.get(f"/hub/api/production?client_id={owner_id}")
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload.get("fallback_all_clients") is True
        usernames = {row.get("username") for row in payload.get("logs", [])}
        assert {"scope_owner", "scope_other"}.issubset(usernames)

        client_db.delete_client(owner_id)
        client_db.delete_client(other_id)
