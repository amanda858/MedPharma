"""Claims Queue visibility rules.

The Claims Queue (GET /hub/api/claims) must enforce who can see which claims:

  * admin  — full cross-account view (the admin report sees everything).
  * staff  — billers see the claims they personally own/billed (Owner), plus
             any unassigned claims (no Owner) they can pick up.
  * client — a lab/practice login sees every claim on its own account
             (regardless of which biller owns it), and a forged client_id
             must NOT expose another lab's claims.
"""
import importlib
import os
import sys

import pytest


@pytest.fixture
def env(tmp_path):
    os.environ["DB_PATH"] = str(tmp_path / "hub.db")
    for mod in ("app.config", "app.client_db", "app.hub_app"):
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    cdb = importlib.import_module("app.client_db")
    cdb = importlib.reload(cdb)
    cdb._CLIENTS_SEED_PATH = str(tmp_path / "clients_seed.json")
    open(cdb._CLIENTS_SEED_PATH, "w").write("[]\n")
    hub = importlib.import_module("app.hub_app")
    hub = importlib.reload(hub)
    from fastapi.testclient import TestClient
    return cdb, TestClient(hub.app)


def _keys(client, username, password):
    client.post("/hub/api/logout")
    r = client.post("/hub/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, f"login {username}: {r.status_code} {r.text}"
    d = client.get("/hub/api/claims").json()
    return sorted(c["ClaimKey"] for c in d["claims"])


def test_queue_visibility_by_role(env):
    cdb, client = env
    with client:
        cid = cdb.create_client({
            "company": "Lab X", "contact_name": "Lab X", "email": "labx@example.com",
            "phone": "555-1", "role": "client", "username": "labx", "password": "labpass12345",
        })
        cdb.create_client({
            "username": "susan", "password": "susanpass12345", "company": "MedPharma SC",
            "contact_name": "Susan", "email": "susan@medprosc.com", "phone": "555-2", "role": "staff",
        })
        cdb.create_client({
            "username": "melissa", "password": "melissapass12345", "company": "MedPharma SC",
            "contact_name": "Melissa", "email": "melissa@medprosc.com", "phone": "555-3", "role": "staff",
        })
        # Three claims on the SAME account, owned by two different billers.
        cdb.create_claim({"client_id": cid, "ClaimKey": "CLM-S1", "ChargeAmount": 500,
                          "BalanceRemaining": 500, "ClaimStatus": "Billed/Submitted", "Owner": "susan"})
        cdb.create_claim({"client_id": cid, "ClaimKey": "CLM-S2", "ChargeAmount": 300,
                          "BalanceRemaining": 300, "ClaimStatus": "Denied", "Owner": "susan"})
        cdb.create_claim({"client_id": cid, "ClaimKey": "CLM-M1", "ChargeAmount": 900,
                          "BalanceRemaining": 900, "ClaimStatus": "A/R Follow-Up", "Owner": "melissa"})
        # An unassigned claim (no Owner yet) any biller may pick up.
        cdb.create_claim({"client_id": cid, "ClaimKey": "CLM-NEW", "ChargeAmount": 150,
                          "BalanceRemaining": 150, "ClaimStatus": "Intake", "Owner": ""})

        # admin sees everything; each biller sees their own work PLUS unassigned.
        assert _keys(client, "admin", "admin123") == ["CLM-M1", "CLM-NEW", "CLM-S1", "CLM-S2"]
        assert _keys(client, "susan", "susanpass12345") == ["CLM-NEW", "CLM-S1", "CLM-S2"]
        assert _keys(client, "melissa", "melissapass12345") == ["CLM-M1", "CLM-NEW"]
        # The lab account sees all of its own claims regardless of owner.
        assert _keys(client, "labx", "labpass12345") == ["CLM-M1", "CLM-NEW", "CLM-S1", "CLM-S2"]
        client.post("/hub/api/logout")


def test_forged_client_id_cannot_cross_accounts(env):
    cdb, client = env
    with client:
        a = cdb.create_client({"company": "Lab A", "contact_name": "A", "email": "a@x.com",
                               "phone": "1", "role": "client", "username": "laba", "password": "labapass12345"})
        b = cdb.create_client({"company": "Lab B", "contact_name": "B", "email": "b@x.com",
                               "phone": "2", "role": "client", "username": "labb", "password": "labbpass12345"})
        cdb.create_claim({"client_id": a, "ClaimKey": "A-1", "ChargeAmount": 100,
                          "BalanceRemaining": 100, "ClaimStatus": "Intake", "Owner": ""})
        cdb.create_claim({"client_id": b, "ClaimKey": "B-1", "ChargeAmount": 200,
                          "BalanceRemaining": 200, "ClaimStatus": "Intake", "Owner": ""})

        client.post("/hub/api/login", json={"username": "laba", "password": "labapass12345"})
        own = sorted(c["ClaimKey"] for c in client.get("/hub/api/claims").json()["claims"])
        forged = sorted(c["ClaimKey"] for c in
                        client.get(f"/hub/api/claims?client_id={b}").json()["claims"])
        client.post("/hub/api/logout")

    assert own == ["A-1"]
    # Forging Lab B's id must still only return Lab A's own claims.
    assert forged == ["A-1"]
