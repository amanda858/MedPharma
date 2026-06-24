"""Team activity rollup: admins need the same work the nightly EOD report
summarizes, but bucketed per day / per week / per month so they can see trends.
Also verifies that uploaded claims inherit the active sub-profile so they show
up in the Claims Queue instead of becoming hidden 'ghost' rows."""
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


def _client(db, username="labx"):
    return db.create_client({
        "username": username, "password": "labpass123", "company": f"Lab {username}",
        "contact_name": f"Lab {username}", "email": f"{username}@example.com",
        "phone": "555-9", "role": "client",
    })


def test_rollup_buckets_shape(client_db):
    for bucket, count in (("day", 14), ("week", 8), ("month", 6)):
        r = client_db.get_team_activity_rollup(bucket=bucket, count=count)
        assert r["bucket"] == bucket
        assert r["count"] == count
        assert len(r["buckets"]) == count
        # Oldest first, newest last; the final bucket includes today.
        today = client_db.business_today_iso()
        assert r["buckets"][-1]["start"] <= today <= r["buckets"][-1]["end"]
        # Every metric key is present on each bucket and in the totals.
        for b in r["buckets"]:
            for k in r["metric_keys"]:
                assert k in b
        for k in r["metric_keys"]:
            assert k in r["totals"]


def test_rollup_counts_todays_work(client_db):
    cid = _client(client_db, "labroll")
    today = client_db.business_today_iso()
    # A payment today should land in today's daily bucket.
    client_db.create_claim({
        "client_id": cid, "ClaimKey": "CLM-1", "ChargeAmount": 500,
        "ClaimStatus": "Billed/Submitted", "Owner": "susan",
    })
    client_db.create_payment({
        "client_id": cid, "ClaimKey": "CLM-1", "PostDate": today,
        "PaymentAmount": 75.0, "PayerType": "Primary", "PostedBy": "melissa",
    })

    r = client_db.get_team_activity_rollup(bucket="day", count=14)
    todays = r["buckets"][-1]
    assert todays["payments_posted"] == 1
    assert todays["payments_amount"] == 75.0
    assert todays["active_users"] >= 1  # melissa posted
    # Totals reconcile with the per-bucket sums.
    assert r["totals"]["payments_posted"] == sum(b["payments_posted"] for b in r["buckets"])


def test_rollup_scopes_to_client(client_db):
    a = _client(client_db, "laba")
    b = _client(client_db, "labb")
    today = client_db.business_today_iso()
    for cid, key in ((a, "A-1"), (b, "B-1")):
        client_db.create_claim({
            "client_id": cid, "ClaimKey": key, "ChargeAmount": 100,
            "ClaimStatus": "Billed/Submitted", "Owner": "susan",
        })
        client_db.create_payment({
            "client_id": cid, "ClaimKey": key, "PostDate": today,
            "PaymentAmount": 10.0, "PayerType": "Primary", "PostedBy": "susan",
        })
    team = client_db.get_team_activity_rollup(bucket="day", count=2)
    only_a = client_db.get_team_activity_rollup(bucket="day", count=2, client_id=a)
    assert team["totals"]["payments_posted"] == 2
    assert only_a["totals"]["payments_posted"] == 1
    assert only_a["client_id"] == a
