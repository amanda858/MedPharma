"""Claim AR balance must stay in sync whenever a money field changes — via
import (covered elsewhere), manual edit, or payment posting — so dashboards,
the AR worklist and the productivity reports never show a stale outstanding
amount. Canonical formula: Balance = Charge - Adjustment - Paid (>= 0)."""
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


def _client(db, username="lab"):
    return db.create_client({
        "username": username, "password": "labpass123", "company": "Lab",
        "contact_name": "Lab", "email": f"{username}@example.com",
        "phone": "555-0", "role": "client",
    })


def _claim(db, cid, **over):
    data = {"client_id": cid, "ClaimKey": "CLM-1", "ChargeAmount": 500,
            "ClaimStatus": "Billed/Submitted", "Owner": "susan"}
    data.update(over)
    db.create_claim(data)
    return db.get_claims(cid)[0]


def test_edit_charge_recomputes_balance(client_db):
    cid = _client(client_db, "lab1")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    # User raises the charge but does not touch the balance field.
    client_db.update_claim(claim["id"], {"ChargeAmount": 750})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 750


def test_edit_paid_recomputes_balance(client_db):
    cid = _client(client_db, "lab2")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    client_db.update_claim(claim["id"], {"PaidAmount": 200})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 300


def test_explicit_balance_is_respected(client_db):
    cid = _client(client_db, "lab3")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    # When the caller passes an explicit balance, trust it (do not override).
    client_db.update_claim(claim["id"], {"ChargeAmount": 750, "BalanceRemaining": 123})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 123


def test_paid_status_zeros_balance(client_db):
    cid = _client(client_db, "lab4")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    client_db.update_claim(claim["id"], {"PaidAmount": 100, "ClaimStatus": "Paid"})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 0


def test_status_only_paid_zeros_balance(client_db):
    cid = _client(client_db, "lab4b")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    # Biller only flips the status to Paid (no money field touched). The AR
    # must still drop to zero so Outstanding AR reflects the resolved claim.
    client_db.update_claim(claim["id"], {"ClaimStatus": "Paid"})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 0


def test_status_only_closed_zeros_balance(client_db):
    cid = _client(client_db, "lab4c")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    client_db.update_claim(claim["id"], {"ClaimStatus": "Closed"})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 0


def test_reopening_claim_restores_balance(client_db):
    cid = _client(client_db, "lab4d")
    claim = _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    client_db.update_claim(claim["id"], {"ClaimStatus": "Paid"})
    assert client_db.get_claim(claim["id"])["BalanceRemaining"] == 0
    # Re-opening the claim (e.g. payment reversed) restores the outstanding AR.
    client_db.update_claim(claim["id"], {"ClaimStatus": "A/R Follow-Up"})
    refreshed = client_db.get_claim(claim["id"])
    assert refreshed["BalanceRemaining"] == 500


def test_payment_adjustment_reduces_balance(client_db):
    cid = _client(client_db, "lab5")
    _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    today = client_db.business_today_iso()
    # Payer pays 400 and writes off the remaining 100 as a contractual adjustment.
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 400.0, "AdjustmentAmount": 100.0,
        "PayerType": "Primary", "PostedBy": "susan",
    })
    claim = client_db.get_claims(cid)[0]
    assert claim["PaidAmount"] == 400
    assert claim["BalanceRemaining"] == 0


def test_delete_payment_restores_balance(client_db):
    cid = _client(client_db, "lab6")
    _claim(client_db, cid, ChargeAmount=500, BalanceRemaining=500)
    today = client_db.business_today_iso()
    pid = client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 200.0, "AdjustmentAmount": 50.0,
        "PayerType": "Primary", "PostedBy": "susan",
    })
    claim = client_db.get_claims(cid)[0]
    assert claim["BalanceRemaining"] == 250
    client_db.delete_payment(pid)
    claim = client_db.get_claims(cid)[0]
    assert claim["PaidAmount"] == 0
    assert claim["BalanceRemaining"] == 500


def test_claim_level_adjustment_counts_with_payment(client_db):
    cid = _client(client_db, "lab7")
    # Claim imported with a 50 adjustment already on it.
    _claim(client_db, cid, ChargeAmount=500, AdjustmentAmount=50, BalanceRemaining=450)
    today = client_db.business_today_iso()
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 300.0, "AdjustmentAmount": 100.0,
        "PayerType": "Primary", "PostedBy": "susan",
    })
    claim = client_db.get_claims(cid)[0]
    # 500 - 50 (claim adj) - 100 (posted adj) - 300 (paid) = 50
    assert claim["BalanceRemaining"] == 50
