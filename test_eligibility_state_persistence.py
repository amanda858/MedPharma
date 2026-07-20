"""Focused temporary-DB test for eligibility lifecycle persistence."""
import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

from eligibility_hybrid.lifecycle import build_eligibility_lifecycle


def test_lifecycle_state_round_trip():
    with tempfile.TemporaryDirectory(prefix="elig-state-") as tmp:
        root = Path(tmp)
        os.environ["DB_PATH"] = str(root / "hub.db")
        os.environ["CLIENTS_SEED_PATH"] = str(root / "clients_seed.json")
        (root / "clients_seed.json").write_text("[]\n", encoding="utf-8")
        if "app.config" in sys.modules:
            importlib.reload(sys.modules["app.config"])
        import app.client_db as client_db
        client_db = importlib.reload(client_db)
        client_db.init_client_hub_db()

        conn = client_db.get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO clients (username,password,salt,company,role,is_active) "
            "VALUES (?,?,?,?,?,1)",
            ("state-test", "x", "y", "State Test", "client"),
        )
        client_id = cur.lastrowid
        conn.commit()
        conn.close()

        state = build_eligibility_lifecycle({
            "record_id": None,
            "client_id": client_id,
            "patient_name": "Example, Patient",
            "dob": "1980-01-01",
            "payer_name": "Example Payer",
            "member_id": "MEMBER1234",
            "requested_services": ["87631"],
        }, source="rule-intercept", configured=False)
        record_id = client_db.create_eligibility({
            "client_id": client_id,
            "PatientName": "Example, Patient",
            "DOB": "1980-01-01",
            "Payor": "Example Payer",
            "MemberID": "MEMBER1234",
            "RequestedServices": "87631",
            "EligibilityStateJson": json.dumps(state, separators=(",", ":")),
        })
        record = client_db.get_eligibility_one(record_id)
        assert record["EligibilityState"]["TRACK"]["client_id"] == client_id

        check_id = client_db.record_eligibility_check({
            "eligibility_id": record_id,
            "client_id": client_id,
            "source": "rule-intercept",
            "status": "Policy Clear",
        })
        assert client_db.finalize_eligibility_check_state(
            check_id, json.dumps(state, separators=(",", ":"))
        )
        assert not client_db.finalize_eligibility_check_state(check_id, "{}")
        checks = client_db.get_eligibility_checks(record_id)
        assert checks[0]["id"] == check_id
        assert checks[0]["engine_state"]["APPROVE"]["approved"] is False


if __name__ == "__main__":
    test_lifecycle_state_round_trip()
    print("eligibility lifecycle persistence test passed")