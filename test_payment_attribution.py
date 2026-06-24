"""Payment-posting attribution: the team is paid on production + payments posted,
so a posted payment must be credited to the poster and surfaced in the
production report, daily snapshot, and EOD team report."""
import importlib
import os
import sys

import pytest


@pytest.fixture
def client_db(tmp_path):
    os.environ["DB_PATH"] = str(tmp_path / "hub.db")
    for mod in ("app.config", "app.client_db"):
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    db = importlib.import_module("app.client_db")
    db = importlib.reload(db)
    db._CLIENTS_SEED_PATH = str(tmp_path / "clients_seed.json")
    db.init_client_hub_db()
    return db


def _seed_claim(db, client_id, key="CLM-1"):
    db.create_claim({
        "client_id": client_id,
        "ClaimKey": key,
        "ChargeAmount": 500,
        "ClaimStatus": "Billed/Submitted",
        "Owner": "susan",
    })


def test_create_payment_records_poster(client_db):
    cid = client_db.create_client({
        "username": "lab1", "password": "labpass123", "company": "Lab One",
        "contact_name": "Lab One", "email": "lab1@example.com",
        "phone": "555-1", "role": "client",
    })
    _seed_claim(client_db, cid)
    today = client_db.business_today_iso()
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 250.0, "PayerType": "Primary", "PostedBy": "susan",
    })
    pays = client_db.get_payments(cid, "CLM-1")
    assert len(pays) == 1
    assert pays[0]["PostedBy"] == "susan"
    assert pays[0]["PaymentAmount"] == 250.0


def test_production_report_surfaces_payments(client_db):
    cid = client_db.create_client({
        "username": "lab2", "password": "labpass123", "company": "Lab Two",
        "contact_name": "Lab Two", "email": "lab2@example.com",
        "phone": "555-2", "role": "client",
    })
    _seed_claim(client_db, cid)
    today = client_db.business_today_iso()
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 100.0, "PayerType": "Primary", "PostedBy": "susan",
    })
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 60.0, "PayerType": "Secondary", "PostedBy": "susan",
    })
    rpt = client_db.get_production_report(cid, today, today)
    susan = next((u for u in rpt["by_user"] if u["username"] == "susan"), None)
    assert susan is not None, "poster with no production log must still appear"
    assert susan["payments_posted"] == 2
    assert susan["payments_amount"] == 160.0
    assert rpt["payments_total_count"] == 2
    assert rpt["payments_total_amount"] == 160.0
    assert len(rpt["payment_details"]) == 2


def test_snapshot_and_eod_credit_poster(client_db):
    cid = client_db.create_client({
        "username": "lab3", "password": "labpass123", "company": "Lab Three",
        "contact_name": "Lab Three", "email": "lab3@example.com",
        "phone": "555-3", "role": "client",
    })
    _seed_claim(client_db, cid)
    today = client_db.business_today_iso()
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 75.0, "PayerType": "Primary", "PostedBy": "melissa",
    })

    snap = client_db.get_user_production_snapshot(today)
    assert snap["payments_by_user"].get("melissa", {}).get("payments_posted") == 1
    assert snap["payments_total_amount"] == 75.0

    eod = client_db.get_eod_team_report(today)
    assert "Payments" in eod["tab_keys"]
    assert eod["headlines"]["payments_posted"] == 1
    melissa = next((u for u in eod["users"] if u["username"] == "melissa"), None)
    assert melissa is not None
    assert melissa["totals"].get("Payments") == 1
