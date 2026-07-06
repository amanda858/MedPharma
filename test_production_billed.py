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


def test_per_biller_self_view_billed_posted_paid_across_accounts(cdb):
    """A biller's self-view (username scope) shows only THEIR own Billed (charges
    out, by Owner), Posted (# payments they posted), and Paid ($ collected), and
    it spans every account they work — not just one client. Other billers'
    numbers must never leak in."""
    a = cdb.create_client({"company": "Lab A", "contact_name": "A", "email": "a@x.com",
                           "phone": "1", "role": "client", "username": "laba",
                           "password": "labapass12345"})
    b = cdb.create_client({"company": "Lab B", "contact_name": "B", "email": "b@x.com",
                           "phone": "2", "role": "client", "username": "labb",
                           "password": "labbpass12345"})
    cdb.create_client({"username": "susan", "password": "susanpass12345",
                       "company": "MedPharma SC", "contact_name": "Susan Smith",
                       "email": "susan@medprosc.com", "phone": "5", "role": "staff"})
    cdb.create_client({"username": "melissa", "password": "melissapass12345",
                       "company": "MedPharma SC", "contact_name": "Melissa",
                       "email": "melissa@medprosc.com", "phone": "6", "role": "staff"})

    # Susan bills across BOTH accounts; Melissa bills on Lab A.
    cdb.create_claim({"client_id": a, "ClaimKey": "A-1", "ChargeAmount": 400,
                      "ClaimStatus": "Billed/Submitted", "Owner": "Susan Smith",
                      "BillDate": "2026-06-05"})
    cdb.create_claim({"client_id": b, "ClaimKey": "B-1", "ChargeAmount": 700,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "BillDate": "2026-06-06"})
    cdb.create_claim({"client_id": a, "ClaimKey": "A-2", "ChargeAmount": 999,
                      "ClaimStatus": "Billed/Submitted", "Owner": "melissa",
                      "BillDate": "2026-06-06"})

    # Susan posts two payments (one per account); Melissa posts one.
    cdb.create_payment({"client_id": a, "ClaimKey": "A-1", "PostDate": "2026-06-07",
                        "PaymentAmount": 250, "PostedBy": "susan"})
    cdb.create_payment({"client_id": b, "ClaimKey": "B-1", "PostDate": "2026-06-08",
                        "PaymentAmount": 300, "PostedBy": "susan"})
    cdb.create_payment({"client_id": a, "ClaimKey": "A-2", "PostDate": "2026-06-08",
                        "PaymentAmount": 500, "PostedBy": "melissa"})

    # Susan's self-view: only her work, across both Lab A and Lab B.
    susan_view = cdb.get_production_report(None, "2026-06-01", "2026-06-30",
                                           username="susan")
    assert susan_view["is_self_view"] is True
    assert susan_view["scope_username"] == "susan"
    # Exactly one row — herself.
    assert [u["username"] for u in susan_view["by_user"]] == ["susan"]
    s = _user(susan_view, "susan")
    # Billed: both accounts, both Owner spellings -> 2 claims / $1100.
    assert s["claims_billed"] == 2
    assert s["claims_billed_amount"] == 1100.0
    # Posted: 2 payments she posted. Paid: $550 collected.
    assert s["payments_posted"] == 2
    assert s["payments_amount"] == 550.0
    # Melissa's $999 billed / $500 paid must NOT appear in Susan's totals.
    assert susan_view["billed_total_amount"] == 1100.0
    assert susan_view["payments_total_amount"] == 550.0

    # Admin combined roll-up still sees everyone.
    everyone = cdb.get_production_report(None, "2026-06-01", "2026-06-30")
    assert everyone["is_self_view"] is False
    assert everyone["billed_total_amount"] == 1100.0 + 999.0
    assert everyone["payments_total_amount"] == 550.0 + 500.0
    assert _user(everyone, "melissa")["payments_amount"] == 500.0


def test_denied_and_rolling_ar(cdb):
    """The report surfaces Denied claims (by Denied Date, credited to the Owner)
    and a Rolling AR figure — the still-open balance on claims dated BEFORE the
    production window start (or with no service date). Current-window DOS balance
    is NOT part of Rolling AR, and Rolling AR ignores the report date range."""
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

    # A submitted (not denied) claim and a denied claim, both in-window.
    cdb.create_claim({"client_id": cid, "ClaimKey": "S-1", "ChargeAmount": 500,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "DOS": "2026-06-19", "BillDate": "2026-06-19",
                      "BalanceRemaining": 0})
    cdb.create_claim({"client_id": cid, "ClaimKey": "D-1", "ChargeAmount": 400,
                      "ClaimStatus": "Denied", "Owner": "Susan Smith",
                      "DOS": "2026-06-20", "BillDate": "2026-06-20",
                      "DeniedDate": "2026-06-22", "BalanceRemaining": 0})

    # Rolling AR backlog: a legacy DOS (before cutoff) and a blank-DOS claim both
    # carry open balance; a current-window DOS balance must be excluded.
    cdb.create_claim({"client_id": cid, "ClaimKey": "AR-OLD", "ChargeAmount": 1234,
                      "ClaimStatus": "A/R Follow-Up", "Owner": "susan",
                      "DOS": "2026-05-01", "BalanceRemaining": 1234})
    cdb.create_claim({"client_id": cid, "ClaimKey": "AR-BLANK", "ChargeAmount": 50,
                      "ClaimStatus": "A/R Follow-Up", "Owner": "susan",
                      "DOS": "", "BalanceRemaining": 50})
    cdb.create_claim({"client_id": cid, "ClaimKey": "AR-NEW", "ChargeAmount": 999,
                      "ClaimStatus": "A/R Follow-Up", "Owner": "susan",
                      "DOS": "2026-06-20", "BalanceRemaining": 999})

    rep = cdb.get_production_report(cid, "2026-06-18", "2026-06-30")
    # Submitted = both claims that went out the door in the window.
    assert rep["billed_total_count"] == 2
    assert rep["billed_total_amount"] == 900.0
    # Denied = just the denied claim, by its Denied Date.
    assert rep["denied_total_count"] == 1
    assert rep["denied_total_amount"] == 400.0
    susan = _user(rep, "susan")
    assert susan["claims_denied"] == 1
    assert susan["claims_denied_amount"] == 400.0
    # Rolling AR = legacy + blank DOS balance; current-window DOS excluded.
    assert rep["rolling_ar"] == 1284.0
    assert rep["rolling_ar_cutoff"] == "2026-06-18"

    # Rolling AR is a backlog snapshot — independent of the report date range.
    wide = cdb.get_production_report(cid, "2026-01-01", "2026-12-31")
    assert wide["rolling_ar"] == 1284.0


def test_paid_comes_from_claim_paid_amount_not_just_payments_table(cdb):
    """Paid $ must reflect the real money paid ON the claims (PaidAmount from the
    uploaded data), credited to the biller (Owner) and attributed by Paid Date.
    This is what lets collections show up even when no payment was manually
    'posted' in the system. Posted (payments table) stays a separate number."""
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

    # Two paid claims (paid in-window) and one with no payment yet. NO rows are
    # added to the payments table, mirroring uploads that carry a Paid column.
    cdb.create_claim({"client_id": cid, "ClaimKey": "P-1", "ChargeAmount": 500,
                      "ClaimStatus": "Paid", "Owner": "susan",
                      "BillDate": "2026-06-19", "PaidDate": "2026-06-21",
                      "PaidAmount": 120, "BalanceRemaining": 380})
    cdb.create_claim({"client_id": cid, "ClaimKey": "P-2", "ChargeAmount": 300,
                      "ClaimStatus": "Paid", "Owner": "Susan Smith",
                      "BillDate": "2026-06-20", "PaidDate": "2026-06-22",
                      "PaidAmount": 80, "BalanceRemaining": 220})
    cdb.create_claim({"client_id": cid, "ClaimKey": "P-3", "ChargeAmount": 200,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "BillDate": "2026-06-20", "PaidAmount": 0,
                      "BalanceRemaining": 200})

    rep = cdb.get_production_report(cid, "2026-06-18", "2026-06-30")
    # Paid = 120 + 80 collected from the claim data, even with an empty
    # payments table (Posted count therefore stays 0).
    assert rep["paid_total_amount"] == 200.0
    assert rep["paid_total_count"] == 2
    assert rep["payments_total_count"] == 0
    susan = _user(rep, "susan")
    assert susan["claims_paid_amount"] == 200.0
    assert susan["claims_paid"] == 2

    # A payment older than the window (by Paid Date) drops out.
    narrow = cdb.get_production_report(cid, "2026-06-22", "2026-06-30")
    assert narrow["paid_total_amount"] == 80.0


def test_denial_recovery_ties_sender_to_reworker(cdb):
    """Denial Rework Accountability re-slices the SAME denied set two ways:
    ``by_sender`` (the original Owner who produced the denied claim — "who is
    slipping") and ``by_reworker`` (the uploaded_by biller who reworked/rebilled
    it — "what was fixed"). It must reconcile EXACTLY with the report's
    ``denied_total`` (never inflating billed), rank senders by dollars, and only
    surface in the admin/comprehensive view — not a biller's self-view."""
    cid = cdb.create_client({
        "company": "SV Diagnostics", "contact_name": "SV Diagnostics",
        "email": "sv@example.com", "phone": "555-0", "role": "client",
        "username": "svdiag", "password": "svpass123456",
    })
    # jessica (the reworker) is an auto-seeded MedPharma staff login.

    # Three denied claims owned by two ORIGINAL senders (free-text Owner names
    # that aren't hub users), all reworked & rebilled by Jessica (uploaded_by).
    cdb.create_claim({"client_id": cid, "ClaimKey": "D-1", "ChargeAmount": 100,
                      "ClaimStatus": "Denied", "Owner": "STEPHANIE SHEPPARD",
                      "BillDate": "2026-06-05", "DeniedDate": "2026-06-06"})
    cdb.create_claim({"client_id": cid, "ClaimKey": "D-2", "ChargeAmount": 50,
                      "ClaimStatus": "Denied", "Owner": "STEPHANIE SHEPPARD",
                      "BillDate": "2026-06-05", "DeniedDate": "2026-06-07"})
    cdb.create_claim({"client_id": cid, "ClaimKey": "D-3", "ChargeAmount": 200,
                      "ClaimStatus": "Denied", "Owner": "Anthony Cesario",
                      "BillDate": "2026-06-05", "DeniedDate": "2026-06-06"})
    # A clean (non-denied) billed claim must never enter denial recovery.
    cdb.create_claim({"client_id": cid, "ClaimKey": "C-1", "ChargeAmount": 900,
                      "ClaimStatus": "Billed/Submitted", "Owner": "susan",
                      "BillDate": "2026-06-10"})

    # uploaded_by is stamped at import time, not by create_claim — set it here.
    conn = cdb.get_db()
    conn.execute("UPDATE claims_master SET uploaded_by=? "
                 "WHERE ClaimKey IN ('D-1','D-2','D-3')", ("jessica",))
    conn.execute("UPDATE claims_master SET uploaded_by=? WHERE ClaimKey='C-1'", ("susan",))
    conn.commit()
    conn.close()

    rep = cdb.get_production_report(cid)
    dr = rep["denial_recovery"]

    # Reconciles EXACTLY with the denied totals — same set, no inflation.
    assert dr["total_count"] == rep["denied_total_count"] == 3
    assert dr["total_amount"] == rep["denied_total_amount"] == 350.0

    # by_reworker: Jessica fixed all three.
    assert dr["by_reworker"] == [{"reworker": "jessica", "count": 3, "amount": 350.0}]

    # by_sender: ranked by dollars desc — Anthony ($200) ahead of Stephanie ($150).
    assert [s["sender"] for s in dr["by_sender"]] == ["Anthony Cesario", "STEPHANIE SHEPPARD"]
    steph = next(s for s in dr["by_sender"] if s["sender"] == "STEPHANIE SHEPPARD")
    assert steph["count"] == 2 and steph["amount"] == 150.0
    anthony = next(s for s in dr["by_sender"] if s["sender"] == "Anthony Cesario")
    assert anthony["count"] == 1 and anthony["amount"] == 200.0
    # The clean biller never appears as a sender.
    assert "susan" not in [s["sender"] for s in dr["by_sender"]]

    # A biller's self-view still computes the block but stays scoped to their own
    # denials (used for their card only) — Jessica has no OWNED denials here.
    self_view = cdb.get_production_report(None, username="jessica")
    assert self_view["is_self_view"] is True
    assert self_view["denial_recovery"]["total_count"] == 0
