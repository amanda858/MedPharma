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

def _bill_dates(client_db, cid):
    conn = client_db.get_db()
    return {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT ClaimKey, BillDate FROM claims_master WHERE client_id=?", (cid,)
        )
    }


def test_billed_claim_without_bill_date_is_stamped_from_dos(hub_env):
    """A claim whose status says it was billed but whose file carries no bill-date
    column must still get a Bill Date (DOS preferred) so it appears in every dated
    billed/production report instead of silently reading $0."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status"],
        ["CLM5001", "2026-02-10", "99213", "150.00", "Billed"],
    ]
    client_routes._import_claims_from_excel(_csv_bytes(rows), ".csv", cid)

    assert _bill_dates(client_db, cid)["CLM5001"] == "2026-02-10"


def test_billed_claim_without_dos_falls_back_to_today(hub_env):
    """When a billed claim has neither a bill date nor a DOS, stamp the import date
    so it is never invisible to billed reports."""
    client_db, client_routes = hub_env
    from app.client_routes import business_today_iso

    cid = _make_client(client_db)
    rows = [
        ["Claim Number", "CPT", "Charge", "Status"],
        ["CLM5002", "99214", "220.00", "Billed/Submitted"],
    ]
    client_routes._import_claims_from_excel(_csv_bytes(rows), ".csv", cid)

    assert _bill_dates(client_db, cid)["CLM5002"] == business_today_iso()


def test_pre_bill_claim_keeps_blank_bill_date(hub_env):
    """A claim still in a pre-bill status (Intake/Verification/Coding) legitimately
    has no Bill Date and must NOT be stamped."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status"],
        ["CLM5003", "2026-02-11", "99215", "300.00", "Intake"],
    ]
    client_routes._import_claims_from_excel(_csv_bytes(rows), ".csv", cid)

    assert _bill_dates(client_db, cid)["CLM5003"] == ""


def test_existing_bill_date_is_preserved_on_reimport(hub_env):
    """Re-uploading a file must not churn a Bill Date that is already set — the
    first real (or stamped) date stands so production history stays stable."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    first = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status", "Bill Date"],
        ["CLM5004", "2026-02-12", "99213", "150.00", "Billed", "2026-02-15"],
    ]
    client_routes._import_claims_from_excel(_csv_bytes(first), ".csv", cid)
    assert _bill_dates(client_db, cid)["CLM5004"] == "2026-02-15"

    # Re-upload the same claim with NO bill-date column — the existing date stands.
    second = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status"],
        ["CLM5004", "2026-02-12", "99213", "150.00", "Billed"],
    ]
    client_routes._import_claims_from_excel(_csv_bytes(second), ".csv", cid)
    assert _bill_dates(client_db, cid)["CLM5004"] == "2026-02-15"


def test_backfill_coerces_non_iso_and_repairs_malformed_dates(hub_env):
    """The startup backfill must always produce ISO Bill Dates the dated reports
    can parse: a non-ISO DOS falls through to the creation date, and an existing
    malformed Bill Date on a billed claim is repaired — while pre-bill claims and
    valid dates are left untouched. Must also be idempotent."""
    from datetime import date
    client_db, _ = hub_env
    cid = _make_client(client_db)

    conn = client_db.get_db()
    conn.executemany(
        "INSERT INTO claims_master (client_id,ClaimKey,DOS,ClaimStatus,BillDate,created_at) "
        "VALUES (?,?,?,?,?,?)",
        [
            (cid, "BK_A", "2026-06-18", "Billed/Submitted", "", "2026-06-01 10:00:00"),
            (cid, "BK_B", "", "Billed/Submitted", "", "2026-06-02 10:00:00"),
            (cid, "BK_C", "2026-06-19", "Intake", "", "2026-06-03 10:00:00"),
            (cid, "BK_D", "2026-06-20", "Paid", "2026-06-21", "2026-06-04 10:00:00"),
            (cid, "BK_E", "06/22/2026", "Rejected", "", "2026-06-05 10:00:00"),
            (cid, "BK_F", "2026-06-23", "Billed/Submitted", "06/15/2026", "2026-06-06 10:00:00"),
        ],
    )
    conn.commit()
    conn.close()

    first = client_db.backfill_missing_bill_dates()
    assert first == 4, "blank x2 + non-ISO blank + malformed = 4 billed rows repaired"
    assert client_db.backfill_missing_bill_dates() == 0, "must be idempotent"

    conn = client_db.get_db()
    got = {
        r["ClaimKey"]: (r["BillDate"] or "")
        for r in conn.execute(
            "SELECT ClaimKey, BillDate FROM claims_master WHERE client_id=?", (cid,)
        )
    }
    conn.close()

    assert got["BK_A"] == "2026-06-18"          # ISO DOS preserved
    assert got["BK_B"] == "2026-06-02"          # no DOS -> creation date
    assert got["BK_C"] == ""                    # pre-bill stays blank
    assert got["BK_D"] == "2026-06-21"          # valid date untouched
    assert got["BK_E"] == "2026-06-05"          # non-ISO DOS -> creation date (ISO)
    assert got["BK_F"] == "2026-06-23"          # malformed -> repaired from DOS

    for key, bd in got.items():
        if bd:
            date.fromisoformat(bd)  # raises if any stamped value is not ISO
