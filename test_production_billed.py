"""Comprehensive production reporting — claims billed credited to the biller.

The Team Production Report must credit each biller (the claim's ``Owner``) for
the claims they put out the door and their charged value, by Bill Date. This is
cumulative production (it accrues across the whole window) and is deliberately
separate from the Claims Queue A/R balance:

  * Per-user ``claims_billed`` / ``claims_billed_amount`` accrue across days.
  * A narrow window only counts claims whose Bill Date falls inside it.
  * An open (all-time) window yields the comprehensive total since day one.
  * Owner display names ("Susan Smith") line up with the same user's logged
    production rows (keyed by username, e.g. "susan").
  * Report totals reconcile to the sum of the per-user rows.
"""
import importlib
import os
import sys

import pytest


@pytest.fixture
def cdb(tmp_path):
    os.environ["DB_PATH"] = str(tmp_path / "hub.db")
    for mod in ("app.config", "app.client_db"):
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    mod = importlib.import_module("app.client_db")
    mod = importlib.reload(mod)
    mod._CLIENTS_SEED_PATH = str(tmp_path / "clients_seed.json")
    open(mod._CLIENTS_SEED_PATH, "w").write("[]\n")
    mod.init_client_hub_db()
    return mod


def _user(d, username):
    for u in d["by_user"]:
        if u["username"] == username:
            return u
    return None


def test_billed_credited_to_biller_and_accrues(cdb):
    cid = cdb.create_client({
        "company": "SV Diagnostics", "contact_name": "SV Diagnostics",
        "email": "sv@example.com", "phone": "555-0", "role": "client",
        "username": "svdiag", "password": "svpass123456",
    })
    cdb.create_client({
        "username": "susan", "password": "susanpass12345", "company": "MedPharma SC",
        "contact_name": "Susan Smith", "email": "susan@medprosc.com",
        "phone": "555-2", "role": "staff",
    })
    cdb.create_client({
        "username": "melissa", "password": "melissapass12345", "company": "MedPharma SC",
        "contact_name": "Melissa", "email": "melissa@medprosc.com",
        "phone": "555-3", "role": "staff",
    })

    # Susan bills two claims on different days; Owner stored as a display name.
    cdb.create_claim({"client_id": cid, "ClaimKey": "S-1", "ChargeAmount": 500,
                      "ClaimStatus": "Billed/Submitted", "Owner": "Susan Smith",
                      "BillDate": "2026-06-01"})
    cdb.create_claim({"client_id": cid, "ClaimKey": "S-2", "ChargeAmount": 300,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "BillDate": "2026-06-10"})
    # Melissa bills one claim.
    cdb.create_claim({"client_id": cid, "ClaimKey": "M-1", "ChargeAmount": 900,
                      "ClaimStatus": "Billed/Submitted", "Owner": "melissa",
                      "BillDate": "2026-06-10"})
    # An unbilled claim (no BillDate) must not count toward production.
    cdb.create_claim({"client_id": cid, "ClaimKey": "X-1", "ChargeAmount": 1000,
                      "ClaimStatus": "Intake", "Owner": "susan", "BillDate": ""})

    # Comprehensive (all-time) — both of Susan's display-name + username claims
    # collapse onto the single "susan" roster user.
    allt = cdb.get_production_report(cid)
    susan = _user(allt, "susan")
    melissa = _user(allt, "melissa")
    assert susan["claims_billed"] == 2
    assert susan["claims_billed_amount"] == 800.0
    assert melissa["claims_billed"] == 1
    assert melissa["claims_billed_amount"] == 900.0
    # Report totals reconcile to the per-user sum.
    assert allt["billed_total_count"] == 3
    assert allt["billed_total_amount"] == 1700.0

    # Narrow window (2026-06-10 only) counts just that day's billing.
    day = cdb.get_production_report(cid, "2026-06-10", "2026-06-10")
    assert _user(day, "susan")["claims_billed"] == 1
    assert _user(day, "susan")["claims_billed_amount"] == 300.0
    assert _user(day, "melissa")["claims_billed"] == 1
    assert day["billed_total_count"] == 2
    assert day["billed_total_amount"] == 1200.0


def test_billed_scoped_per_account(cdb):
    a = cdb.create_client({"company": "Lab A", "contact_name": "A", "email": "a@x.com",
                           "phone": "1", "role": "client", "username": "laba",
                           "password": "labapass12345"})
    b = cdb.create_client({"company": "Lab B", "contact_name": "B", "email": "b@x.com",
                           "phone": "2", "role": "client", "username": "labb",
                           "password": "labbpass12345"})
    cdb.create_client({"username": "susan", "password": "susanpass12345",
                       "company": "MedPharma SC", "contact_name": "Susan",
                       "email": "susan@medprosc.com", "phone": "5", "role": "staff"})
    cdb.create_claim({"client_id": a, "ClaimKey": "A-1", "ChargeAmount": 400,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "BillDate": "2026-06-05"})
    cdb.create_claim({"client_id": b, "ClaimKey": "B-1", "ChargeAmount": 700,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "BillDate": "2026-06-05"})

    # Selecting Lab A only credits Lab A's billed value.
    only_a = cdb.get_production_report(a)
    assert only_a["billed_total_amount"] == 400.0
    # Cross-account (admin, no client filter) rolls up every account's billing.
    everyone = cdb.get_production_report()
    assert _user(everyone, "susan")["claims_billed"] == 2
    assert everyone["billed_total_amount"] == 1100.0
