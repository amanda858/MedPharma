import importlib
import io
import csv
import os
import sys
from pathlib import Path

import pytest


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
    client_db.init_client_hub_db()
    client_routes = importlib.import_module("app.client_routes")
    client_routes = importlib.reload(client_routes)
    return client_db, client_routes


def _csv_bytes(rows):
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    return buf.getvalue().encode("utf-8")


def _make_client(client_db):
    return client_db.create_client({
        "username": "acme_clinic",
        "password": "acmepass123",
        "company": "Acme Clinic",
        "contact_name": "Acme",
        "email": "acme@example.com",
        "phone": "555-0000",
        "role": "client",
    })


def _totals(client_db, cid):
    conn = client_db.get_db()
    n = conn.execute(
        "SELECT COUNT(*) FROM claims_master WHERE client_id=?", (cid,)
    ).fetchone()[0]
    total = conn.execute(
        "SELECT COALESCE(SUM(ChargeAmount),0) FROM claims_master WHERE client_id=?",
        (cid,),
    ).fetchone()[0]
    return n, total


def test_multi_service_line_claims_are_not_collapsed(hub_env):
    """Several service lines that share one claim number must each persist so the
    admin billed total reflects every charge — not just the last line."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "Patient", "DOS", "CPT", "Charge", "Status"],
        ["CLM1001", "John Doe", "2026-01-05", "99213", "150.00", "Billed"],
        ["CLM1001", "John Doe", "2026-01-05", "85025", "45.00", "Billed"],
        ["CLM1001", "John Doe", "2026-01-05", "36415", "20.00", "Billed"],
        ["CLM1002", "Jane Roe", "2026-01-06", "99214", "220.00", "Billed"],
    ]
    imported, errors = client_routes._import_claims_from_excel(
        _csv_bytes(rows), ".csv", cid
    )
    assert errors == []
    assert imported == 4

    count, total = _totals(client_db, cid)
    assert count == 4, "every service line should persist as its own row"
    assert total == pytest.approx(435.0), "billed total must sum all service lines"


def test_reimporting_same_file_is_idempotent(hub_env):
    """Re-uploading the identical file must update rows in place, never double-count."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status"],
        ["CLM1001", "2026-01-05", "99213", "150.00", "Billed"],
        ["CLM1001", "2026-01-05", "85025", "45.00", "Billed"],
        ["CLM2002", "2026-01-06", "99214", "220.00", "Billed"],
    ]
    content = _csv_bytes(rows)

    client_routes._import_claims_from_excel(content, ".csv", cid)
    client_routes._import_claims_from_excel(content, ".csv", cid)

    count, total = _totals(client_db, cid)
    assert count == 3
    assert total == pytest.approx(415.0)


def test_single_line_claim_keeps_bare_claim_number(hub_env):
    """A claim that appears on only one row keeps its original claim number so the
    UI claim identifier is unchanged for the common case."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status"],
        ["CLM3003", "2026-01-07", "99215", "300.00", "Billed"],
    ]
    client_routes._import_claims_from_excel(_csv_bytes(rows), ".csv", cid)

    conn = client_db.get_db()
    keys = [r[0] for r in conn.execute(
        "SELECT ClaimKey FROM claims_master WHERE client_id=?", (cid,)
    )]
    assert keys == ["CLM3003"]
