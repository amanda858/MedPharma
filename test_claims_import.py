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


def test_structural_match_routes_misfiled_claims_to_claims(hub_env):
    """A claim spreadsheet uploaded under a non-data category (e.g. "General")
    must still be detected as Claims by its column structure, so daily billed
    work isn't silently saved as an inert document."""
    _client_db, client_routes = hub_env

    # Susan's daily worklist: a claim-id column + several claim fields, but the
    # filename/category give no strong keyword signal.
    claims_headers = ["Claim ID", "Patient", "DOS", "CPT", "Charge", "Status"]
    m = client_routes._claims_structural_match(claims_headers)
    assert m["is_claims"] is True, m

    inferred, _debug = client_routes._infer_excel_category(
        _csv_bytes([claims_headers, ["C1", "Jane", "2026-06-20", "99213", "150", "Billed"]]),
        ".csv", filename="LIMS Daily Worklist.xlsx", description="")
    assert inferred == "Claims", "mis-filed claim sheet must auto-route to Claims"

    # A credentialing-style sheet must NOT be mistaken for claims.
    cred_headers = ["Provider", "Payor", "Type", "Status", "Submitted",
                    "Follow Up", "Approved", "Expiration", "Owner", "Notes"]
    assert client_routes._claims_structural_match(cred_headers)["is_claims"] is False


def test_import_stored_file_endpoint_recovers_unimported_spreadsheet(hub_env):
    """The one-click import endpoint must ingest an already-uploaded spreadsheet
    that was saved as a document, and re-file it under Claims."""
    client_db, client_routes = hub_env
    import os as _os
    from fastapi.testclient import TestClient
    hub = importlib.import_module("app.hub_app")
    hub = importlib.reload(hub)
    tc = TestClient(hub.app)

    cid = _make_client(client_db)
    # Simulate a file uploaded as "General" (never imported): write bytes to the
    # upload dir and register the client_files row exactly like a real upload.
    content = _csv_bytes([
        ["Claim ID", "Patient", "DOS", "CPT", "Charge", "Status"],
        ["CLM-D1", "Pat A", "2026-06-20", "99213", "150.00", "Billed"],
        ["CLM-D2", "Pat B", "2026-06-21", "99214", "220.00", "Billed"],
    ])
    fname = "stored_general.csv"
    _os.makedirs(client_routes.UPLOAD_DIR, exist_ok=True)
    with open(_os.path.join(client_routes.UPLOAD_DIR, fname), "wb") as f:
        f.write(content)
    file_id = client_db.add_file(
        client_id=cid, filename=fname, original_name="LIMS Daily Claims Worklist.csv",
        file_type="excel", file_size=len(content), category="General",
        description="", row_count=2, uploaded_by="susan")

    with tc:
        tc.post("/hub/api/login", json={"username": "admin", "password": "admin123"})
        r = tc.post(f"/hub/api/files/{file_id}/import-claims")
        assert r.status_code == 200, r.text
        d = r.json()
        tc.post("/hub/api/logout")

    assert d["imported"] == 2, d
    count, total = _totals(client_db, cid)
    assert count == 2
    assert total == pytest.approx(370.0)
    # File is re-filed under Claims so it's no longer flagged as pending.
    rec = client_db.get_file_record(file_id, cid)
    assert rec["category"] == "Claims"


def test_import_stored_file_rejects_pdf(hub_env):
    """PDFs cannot be imported into claims — the endpoint must say so clearly."""
    client_db, client_routes = hub_env
    import os as _os
    from fastapi.testclient import TestClient
    hub = importlib.import_module("app.hub_app")
    hub = importlib.reload(hub)
    tc = TestClient(hub.app)

    cid = _make_client(client_db)
    _os.makedirs(client_routes.UPLOAD_DIR, exist_ok=True)
    with open(_os.path.join(client_routes.UPLOAD_DIR, "x.pdf"), "wb") as f:
        f.write(b"%PDF-1.4 fake")
    file_id = client_db.add_file(
        client_id=cid, filename="x.pdf", original_name="ERA.pdf",
        file_type="pdf", file_size=12, category="General",
        description="", row_count=0, uploaded_by="melissa")

    with tc:
        tc.post("/hub/api/login", json={"username": "admin", "password": "admin123"})
        r = tc.post(f"/hub/api/files/{file_id}/import-claims")
        tc.post("/hub/api/logout")
    assert r.status_code == 400
    assert "spreadsheet" in r.json()["detail"].lower()


def test_csv_with_title_rows_above_header_still_imports(hub_env):
    """Real exports often have a title/blank row above the header. The parser
    must find the real header row instead of treating the title as columns."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    raw = (
        "SV Diagnostics — Daily Claims Worklist\n"        # title row
        "Generated 2026-06-24\n"                          # subtitle row
        "\n"                                               # blank row
        "Claim ID,Patient,DOS,CPT,Charge,Status\n"
        "CLM-T1,Pat A,2026-06-20,99213,150.00,Billed\n"
        "CLM-T2,Pat B,2026-06-21,99214,220.00,Billed\n"
    ).encode("utf-8")

    imported, errors = client_routes._import_claims_from_excel(raw, ".csv", cid)
    assert errors == [], errors
    assert imported == 2
    count, total = _totals(client_db, cid)
    assert count == 2
    assert total == pytest.approx(370.0)


def test_csv_semicolon_delimited_and_bom_imports(hub_env):
    """A BOM-prefixed, semicolon-delimited CSV (common from Excel in some
    locales) must still parse and import."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    raw = "\ufeffClaim ID;Patient;DOS;CPT;Charge;Status\n" \
          "CLM-S1;Pat A;2026-06-20;99213;150.00;Billed\n" \
          "CLM-S2;Pat B;2026-06-21;99214;220.00;Billed\n"
    imported, errors = client_routes._import_claims_from_excel(
        raw.encode("utf-8"), ".csv", cid)
    assert errors == [], errors
    assert imported == 2
    count, total = _totals(client_db, cid)
    assert count == 2
    assert total == pytest.approx(370.0)


def test_structural_match_recognizes_minimal_claims_and_skips_other_data(hub_env):
    """A minimal claims sheet (no claim-id column) must be recognized, while
    credentialing/enrollment sheets must NOT be misrouted to claims."""
    _client_db, client_routes = hub_env

    # Minimal claims: just DOS + CPT + Charge — no explicit claim id.
    assert client_routes._claims_structural_match(
        ["DOS", "CPT", "Charge"])["is_claims"] is True
    # Patient-centric claims sheet.
    assert client_routes._claims_structural_match(
        ["Patient Name", "Charge Amount", "Balance", "Claim Status"])["is_claims"] is True

    # Other data types must stay out of claims.
    assert client_routes._claims_structural_match(
        ["Provider", "Payor", "Type", "Status", "Submitted", "Approved",
         "Expiration", "Owner", "Notes"])["is_claims"] is False
    assert client_routes._claims_structural_match(
        ["Provider", "Payer", "Effective Date", "Participation", "Network",
         "Status"])["is_claims"] is False
    # A team-production timesheet must not look like claims either.
    assert client_routes._claims_structural_match(
        ["Work Date", "Username", "Category", "Task Description", "Quantity"]
    )["is_claims"] is False


def test_auto_import_sweep_ingests_misfiled_claims_without_a_click(hub_env):
    """The seamless sweep must find claim-shaped spreadsheets saved under a
    non-data category, import them, and re-file them under Claims — with no
    manual import call. Non-claim documents must be left untouched."""
    client_db, client_routes = hub_env
    import os as _os
    cid = _make_client(client_db)
    _os.makedirs(client_routes.UPLOAD_DIR, exist_ok=True)

    # A claim-shaped spreadsheet mis-filed as "General".
    claims = _csv_bytes([
        ["Claim ID", "Patient", "DOS", "CPT", "Charge", "Status"],
        ["SWEEP-1", "Pat A", "2026-06-20", "99213", "150.00", "Billed"],
        ["SWEEP-2", "Pat B", "2026-06-21", "99214", "220.00", "Billed"],
    ])
    with open(_os.path.join(client_routes.UPLOAD_DIR, "daily.csv"), "wb") as f:
        f.write(claims)
    claims_id = client_db.add_file(
        client_id=cid, filename="daily.csv", original_name="LIMS Daily.csv",
        file_type="excel", file_size=len(claims), category="General",
        description="", row_count=2, uploaded_by="susan")

    # A non-claim document that must be ignored by the sweep.
    other = _csv_bytes([
        ["Provider", "Payer", "Effective Date", "Participation", "Status"],
        ["Dr X", "BCBS", "2026-01-01", "In Network", "Approved"],
    ])
    with open(_os.path.join(client_routes.UPLOAD_DIR, "cred.csv"), "wb") as f:
        f.write(other)
    other_id = client_db.add_file(
        client_id=cid, filename="cred.csv", original_name="Credentialing.csv",
        file_type="excel", file_size=len(other), category="Credentialing",
        description="", row_count=1, uploaded_by="admin")

    result = client_routes.auto_import_pending_claim_files(cid)
    assert result["files"] == 1, result
    assert result["rows"] == 2, result

    count, total = _totals(client_db, cid)
    assert count == 2
    assert total == pytest.approx(370.0)

    # The claim file is re-filed under Claims; the credentialing file is untouched.
    assert client_db.get_file_record(claims_id, cid)["category"] == "Claims"
    assert client_db.get_file_record(other_id, cid)["category"] == "Credentialing"

    # Re-running the sweep is idempotent: nothing left to import, no double-count.
    again = client_routes.auto_import_pending_claim_files(cid)
    assert again["files"] == 0
    count2, total2 = _totals(client_db, cid)
    assert count2 == 2
    assert total2 == pytest.approx(370.0)


def test_claim_alerts_are_removed(hub_env):
    """Compliance-style claim alerts (SLA breaches, high/elevated denial rate,
    90+ day AR) must NOT be generated. Only credentialing/enrollment lifecycle
    alerts may remain."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    # Import claims that would historically trip every claim alert: old, denied,
    # unpaid, with open balances well past 90 days.
    rows = [["Claim ID", "Patient", "DOS", "CPT", "Charge", "Balance", "Status",
             "Bill Date", "Denied Date"]]
    for i in range(20):
        rows.append([f"OLD-{i}", f"Pt {i}", "2025-01-01", "99213", "500.00",
                     "500.00", "Denied", "2025-01-02", "2025-01-15"])
    client_routes._import_claims_from_excel(_csv_bytes(rows), ".csv", cid)

    alerts = client_db.get_alerts(cid)
    titles = " ".join(a.get("title", "").lower() for a in alerts)
    assert "sla breach" not in titles, alerts
    assert "denial rate" not in titles, alerts
    assert "90+ day ar" not in titles, alerts


def test_claims_column_map_is_shared_single_source(hub_env):
    """The importer and the mapping diagnostic must use the same mapping
    parameters, so what the diagnostic shows is exactly what the importer does."""
    _client_db, client_routes = hub_env
    # The importer's COLUMN_MAP is the module-level CLAIMS_COLUMN_MAP.
    assert "charge amount" in client_routes.CLAIMS_COLUMN_MAP
    # Real-world clearinghouse/PM aliases now resolve to the right DB columns.
    cm = client_routes.CLAIMS_COLUMN_MAP
    assert client_routes._fuzzy_match_column("Claim Sent Date", cm) == "BillDate"
    assert client_routes._fuzzy_match_column("Patient Control Number", cm) == "ClaimKey"
    assert client_routes._fuzzy_match_column("Billed Charges", cm) == "ChargeAmount"
    assert client_routes._fuzzy_match_column("Insurance Payment", cm) == "PaidAmount"


def test_dashboard_claim_buckets(hub_env):
    """The dashboard exposes the four lifecycle buckets the team monitors —
    Billed (superset), Denied, Paid, Posted — with counts and dollar values."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status", "Bill Date",
         "Denial Reason", "Paid Amount"],
        # Billed only
        ["B1", "2026-06-19", "99213", "150.00", "Billed", "2026-06-19", "", "0"],
        ["B2", "2026-06-20", "99214", "200.00", "Billed", "2026-06-20", "", "0"],
        # Denied (still billed)
        ["D1", "2026-06-21", "99215", "300.00", "Denied", "2026-06-21", "CO-16", "0"],
        # Paid (still billed)
        ["P1", "2026-06-22", "85025", "100.00", "Paid", "2026-06-22", "", "90.00"],
    ]
    imported, errors = client_routes._import_claims_from_excel(
        _csv_bytes(rows), ".csv", cid
    )
    assert errors == []
    assert imported == 4

    d = client_db.get_dashboard(cid)
    cb = d["claim_buckets"]

    # Billed is the SUPERSET — every claim with a Bill Date (all 4).
    assert cb["billed"]["count"] == 4
    assert cb["billed"]["amount"] == pytest.approx(750.0)
    # Denied — status Denied or a denial reason present.
    assert cb["denied"]["count"] == 1
    assert cb["denied"]["amount"] == pytest.approx(300.0)
    # Paid — claim lines with money paid.
    assert cb["paid"]["count"] == 1
    assert cb["paid"]["amount"] == pytest.approx(90.0)

    # Rolling A/R is reported with the team start date.
    assert d["rolling_ar_start"] == "2026-06-18"
    assert "rolling_ar" in d


def _xlsx_bytes(rows, sheet_name="Sheet1"):
    """Build a minimal single-sheet .xlsx in memory from a list of rows."""
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    for r in rows:
        ws.append(list(r))
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_posted_bucket_reflects_posted_payments(hub_env):
    """A posted-payments (LIMS-style) export must populate the dashboard
    'Posted' bucket from the payments table, and stay idempotent on re-import
    so deposits are never double-counted."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["BATCH #", "DEPOSIT DATE", "PAYER NAME", "AMOUNT", "EFT NUMBER"],
        ["B100", "2026-06-20", "Aetna", "1000.00", "EFT001"],
        ["B100", "2026-06-21", "Cigna", "1500.50", "EFT002"],
    ]
    content = _xlsx_bytes(rows)

    # Import twice — Posted must not double-count.
    for _ in range(2):
        imported, errors = client_routes._import_claims_from_excel(
            content, ".xlsx", cid, uploaded_by="susan"
        )
    assert errors == []
    assert imported == 2

    d = client_db.get_dashboard(cid)
    cb = d["claim_buckets"]
    # Posted = real deposits from the payments table.
    assert cb["posted"]["count"] == 2
    assert cb["posted"]["amount"] == pytest.approx(2500.50)


def test_posted_payments_write_idempotent_claim_notes(hub_env):
    """Each posted payment records a per-claim note (with date) so a claim shows
    whether/when its payment was posted, while Paid and Posted stay the same
    dollar figure. Re-importing must not stack duplicate notes."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["BATCH #", "DEPOSIT DATE", "PAYER NAME", "AMOUNT", "EFT NUMBER"],
        ["B1", "2026-06-20", "Aetna", "1000.00", "EFT001"],
        ["B1", "2026-06-21", "Cigna", "1500.50", "EFT002"],
    ]
    content = _xlsx_bytes(rows)

    for _ in range(2):  # import twice — notes must not duplicate
        imported, errors = client_routes._import_claims_from_excel(
            content, ".xlsx", cid, uploaded_by="susan"
        )
    assert errors == []

    conn = client_db.get_db()
    notes = conn.execute(
        "SELECT ClaimKey, Note FROM notes_log WHERE client_id=? AND Module='Payment' ORDER BY ClaimKey",
        (cid,),
    ).fetchall()
    # Exactly one note per posted payment (idempotent across reimports).
    assert len(notes) == 2
    joined = " ".join(n[1] for n in notes)
    # Notes carry the posting date.
    assert "2026-06-20" in joined
    assert "2026-06-21" in joined
    # Posted dollars equal Paid/Posted bucket dollars (same money).
    d = client_db.get_dashboard(cid)
    assert d["claim_buckets"]["posted"]["amount"] == pytest.approx(2500.50)


def test_denied_claims_counted_inside_billed(hub_env):
    """A denied claim is still a billed claim — Denied stays a subset of Billed.
    But a claim still in intake with no Bill Date has NOT gone out the door, so it
    is excluded from Billed Out and surfaced in the separate 'intake' bucket.
    Billed + Intake = every charge loaded, so nothing is lost."""
    client_db, client_routes = hub_env
    cid = _make_client(client_db)

    rows = [
        ["Claim Number", "DOS", "CPT", "Charge", "Status", "Bill Date", "Denial Reason"],
        ["B1", "2026-06-19", "99213", "150.00", "Billed", "2026-06-19", ""],
        ["D1", "2026-06-20", "99214", "200.00", "Denied", "2026-06-20", "CO-16"],
        ["I1", "2026-06-21", "99215", "300.00", "Intake", "", ""],
    ]
    imported, errors = client_routes._import_claims_from_excel(
        _csv_bytes(rows), ".csv", cid
    )
    assert errors == []

    d = client_db.get_dashboard(cid)
    cb = d["claim_buckets"]
    # Billed = claims out the door: the billed one + the denied one (both carry a
    # Bill Date). The intake claim with no Bill Date is NOT billed.
    assert cb["billed"]["count"] == 2
    assert cb["billed"]["amount"] == pytest.approx(350.0)
    # The intake claim is captured separately, never silently dropped.
    assert cb["intake"]["count"] == 1
    assert cb["intake"]["amount"] == pytest.approx(300.0)
    # Billed + Intake accounts for every loaded charge.
    assert cb["billed"]["amount"] + cb["intake"]["amount"] == pytest.approx(650.0)
    # Denied is a subset, never larger than Billed.
    assert cb["denied"]["count"] == 1
    assert cb["denied"]["count"] <= cb["billed"]["count"]
    assert cb["denied"]["amount"] == pytest.approx(200.0)


def test_admin_can_read_posting_notes_on_account_claim(hub_env):
    """Posting notes live under the claim's owning account (e.g. the lab), not
    the admin's own login. An admin browsing claims across accounts must still
    be able to read those per-claim notes — otherwise the posting date the team
    asked for would be invisible."""
    client_db, client_routes = hub_env
    from fastapi.testclient import TestClient
    hub = importlib.import_module("app.hub_app")
    hub = importlib.reload(hub)
    tc = TestClient(hub.app)

    # Claim + posting note land under a NON-admin account (the lab).
    cid = _make_client(client_db)
    rows = [
        ["BATCH #", "DEPOSIT DATE", "PAYER NAME", "AMOUNT", "EFT NUMBER"],
        ["B1", "2026-06-20", "Aetna", "1000.00", "EFT777"],
    ]
    client_routes._import_claims_from_excel(
        _xlsx_bytes(rows), ".xlsx", cid, uploaded_by="susan"
    )

    with tc:
        tc.post("/hub/api/login", json={"username": "admin", "password": "admin123"})
        r = tc.get("/hub/api/notes", params={"claim_key": "PMT-EFT777"})
        tc.post("/hub/api/logout")
    assert r.status_code == 200, r.text
    notes = r.json()
    # Admin resolves to the claim's owning account and sees the dated posting note.
    assert any("2026-06-20" in n.get("Note", "") for n in notes), notes


def test_client_login_scopes_to_assigned_account(hub_env):
    """A client login whose own row id differs from the account holding the
    claims (a dedicated user assigned to one lab account) must see that
    account's claims and dashboard buckets — not an empty self-scoped view.
    Locks the fix so newly-added client logins always resolve to their single
    assigned account."""
    client_db, client_routes = hub_env
    from fastapi.testclient import TestClient
    hub = importlib.import_module("app.hub_app")
    hub = importlib.reload(hub)
    tc = TestClient(hub.app)

    # Lab account owns the billed claims.
    lab_cid = _make_client(client_db)
    if isinstance(lab_cid, dict):
        lab_cid = lab_cid["id"]
    rows = [
        ["Patient", "Claim ID", "Charge Amount", "Bill Date", "Status"],
        ["Jane Doe", "CLM-1", "500.00", "2026-06-20", "Submitted"],
        ["John Roe", "CLM-2", "300.00", "2026-06-21", "Submitted"],
    ]
    client_routes._import_claims_from_excel(
        _xlsx_bytes(rows), ".xlsx", lab_cid, uploaded_by="susan"
    )

    # A SEPARATE client login (different id) assigned to that one account.
    member_id = client_db.create_client({
        "username": "tivany",
        "password": "billout26!x",
        "company": "SV Diagnostics",
        "contact_name": "Tivany",
        "email": "tivany@svd.example.com",
        "phone": "",
        "role": "client",
    })
    if isinstance(member_id, dict):
        member_id = member_id["id"]
    assert int(member_id) != int(lab_cid)
    client_db.set_client_access(lab_cid, [member_id], granted_by="admin")

    with tc:
        r = tc.post("/hub/api/login",
                    json={"username": "tivany", "password": "billout26!x"})
        assert r.status_code == 200, r.text
        accounts = tc.get("/hub/api/accounts")
        dash = tc.get("/hub/api/dashboard")
        claims = tc.get("/hub/api/claims")
        tc.post("/hub/api/logout")

    # The account selector must offer the ASSIGNED lab account (which holds the
    # claims), not the client's own empty login row — otherwise picking the card
    # loads dashboard/client/<own id> and everything reads $0.
    assert accounts.status_code == 200, accounts.text
    acct_ids = [int(a.get("id")) for a in accounts.json()]
    assert int(lab_cid) in acct_ids, acct_ids
    assert int(member_id) not in acct_ids, acct_ids

    assert dash.status_code == 200, dash.text
    buckets = dash.json().get("claim_buckets", {})
    assert buckets.get("billed", {}).get("count", 0) == 2, buckets
    assert buckets.get("billed", {}).get("amount", 0) == 800.0, buckets

    assert claims.status_code == 200, claims.text
    visible = claims.json().get("claims", [])
    assert len(visible) == 2, visible
