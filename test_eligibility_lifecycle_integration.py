"""End-to-end lifecycle checks around the owning verification function."""
import importlib
import json
import os
import sys
import tempfile
from pathlib import Path


ELIGIBILITY_ENV_KEYS = (
    "ELIGIBILITY_PROVIDER_NPI", "ELIGIBILITY_PROVIDER_NAME",
    "ELIGIBILITY_BAA_ATTESTED",
    "STEDI_API_KEY", "STEDI_PROVIDER_NPI", "STEDI_PROVIDER_NAME", "PVERIFY_CLIENT_ID",
    "PVERIFY_CLIENT_SECRET", "HETS_ENDPOINT_URL", "HETS_SUBMITTER_ID",
    "HETS_USERNAME", "HETS_PASSWORD", "HETS_PROVIDER_NPI", "HETS_PROVIDER_NAME",
)


def _bootstrap():
    tmp = tempfile.TemporaryDirectory(prefix="elig-lifecycle-e2e-")
    root = Path(tmp.name)
    os.environ["DB_PATH"] = str(root / "hub.db")
    os.environ["CLIENTS_SEED_PATH"] = str(root / "clients_seed.json")
    for key in ELIGIBILITY_ENV_KEYS:
        os.environ.pop(key, None)
    (root / "clients_seed.json").write_text("[]\n", encoding="utf-8")
    if "app.config" in sys.modules:
        importlib.reload(sys.modules["app.config"])
    import app.client_db as client_db
    client_db = importlib.reload(client_db)
    client_db.init_client_hub_db()
    import app.client_routes as routes
    routes = importlib.reload(routes)

    conn = client_db.get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,role,is_active) "
        "VALUES (?,?,?,?,?,1)",
        ("elig-test", "x", "y", "Eligibility Test", "client"),
    )
    client_id = cur.lastrowid
    conn.commit()
    conn.close()
    return tmp, client_db, routes, client_id


def _record(client_db, client_id, **overrides):
    data = {
        "client_id": client_id,
        "PatientName": "Example, Patient",
        "DOB": "1980-01-01",
        "Payor": "Aetna",
        "MemberID": "MEMBER1234",
        "RequestedServices": "87631 J12.81",
    }
    data.update(overrides)
    record_id = client_db.create_eligibility(data)
    return client_db.get_eligibility_one(record_id)


def test_live_source_requires_sendable_provider_identity():
    tmp, _client_db, routes, _client_id = _bootstrap()
    try:
        routes._require_full_admin = lambda _session: {
            "id": 1, "role": "admin", "username": "admin",
        }
        locked_save = routes.admin_eligibility_credentials_save(
            routes.EligCredsIn(
                stedi_api_key="test-key",
                stedi_provider_npi="1234567893",
                stedi_provider_name="Example Laboratory",
                baa_attested=False,
            ),
            None,
        )
        assert locked_save["stedi_configured"] is True
        assert locked_save["baa_attested"] is False
        assert locked_save["live"] is False
        unlocked_save = routes.admin_eligibility_credentials_save(
            routes.EligCredsIn(baa_attested=True), None
        )
        assert unlocked_save["baa_attested"] is True
        assert unlocked_save["live"] is True

        values = {
            "STEDI_API_KEY": "test-key",
            "STEDI_PROVIDER_NPI": "1234567893",
        }
        routes._elig_cfg = lambda key: values.get(key, "")
        assert routes._elig_source_flags() == {
            "stedi": False, "hets": False, "pverify": False,
        }
        assert routes._build_live_eligibility_provider().configured is False

        values["STEDI_PROVIDER_NAME"] = "Example Laboratory"
        assert routes._elig_source_flags()["stedi"] is True
        assert routes._build_live_eligibility_provider().configured is False
        disconnected = routes.admin_eligibility_config(None)
        assert disconnected["stedi_configured"] is True
        assert disconnected["baa_attested"] is False
        assert disconnected["live"] is False
        assert any("attestation" in item.lower() for item in disconnected["missing"])
        values["ELIGIBILITY_BAA_ATTESTED"] = "true"
        assert routes._build_live_eligibility_provider().name == "stedi"
        connected = routes.admin_eligibility_config(None)
        assert connected["baa_attested"] is True
        assert connected["live"] is True
        assert not any("attestation" in item.lower() for item in connected["missing"])

        values.clear()
        values.update({
            "PVERIFY_CLIENT_ID": "client-id",
            "PVERIFY_CLIENT_SECRET": "client-secret",
        })
        assert routes._elig_source_flags()["pverify"] is False
        values.update({
            "ELIGIBILITY_PROVIDER_NPI": "1234567893",
            "ELIGIBILITY_PROVIDER_NAME": "Example Laboratory",
            "ELIGIBILITY_BAA_ATTESTED": "true",
        })
        assert routes._elig_source_flags()["pverify"] is True
        assert routes._build_live_eligibility_provider().name == "pverify"

        values.clear()
        values.update({
            "HETS_ENDPOINT_URL": "https://hets.example/core",
            "HETS_SUBMITTER_ID": "SUBMIT12345",
            "HETS_USERNAME": "user",
            "HETS_PASSWORD": "password",
        })
        assert routes._elig_source_flags()["hets"] is False
        values.update({
            "HETS_PROVIDER_NPI": "1234567893",
            "HETS_PROVIDER_NAME": "Example Laboratory",
            "ELIGIBILITY_BAA_ATTESTED": "true",
        })
        assert routes._elig_source_flags()["hets"] is True
        assert routes._build_live_eligibility_provider("Medicare Part B").name == "hets"
        assert routes._build_live_eligibility_provider("Aetna").configured is False

        os.environ.update({
            "STEDI_API_KEY": "test-key",
            "STEDI_PROVIDER_NPI": "1234567893",
            "STEDI_PROVIDER_NAME": "Example Laboratory",
            "ELIG_SANDBOX": "0",
        })
        from eligibility_hybrid.config import (
            build_default_engine, build_eligibility_provider,
        )
        assert build_eligibility_provider().configured is False
        assert build_default_engine(allow_live=False).secondary.configured is False
        assert build_default_engine(allow_live=True).secondary.configured is True
        os.environ["ELIGIBILITY_BAA_ATTESTED"] = "true"
        assert build_eligibility_provider().configured is True
    finally:
        for key in ELIGIBILITY_ENV_KEYS + ("ELIG_SANDBOX",):
            os.environ.pop(key, None)
        tmp.cleanup()


def test_missing_input_and_offline_and_live_hold():
    tmp, client_db, routes, client_id = _bootstrap()
    try:
        missing = _record(client_db, client_id, Payor="")
        missing_result = routes._verify_and_record(missing, actor_label="Test")
        assert missing_result["missing_fields"] == ["payer_name"]
        assert missing_result["engine_state"]["OPS"]["status"] == "blocked"
        routes._require_user = lambda _session: {
            "id": 1, "role": "admin", "username": "admin",
        }
        wrapped_missing = routes.verify_elig(missing["id"], None)
        assert wrapped_missing["missing_fields"] == ["payer_name"]
        assert wrapped_missing["engine_state"]["OPS"]["status"] == "blocked"
        assert wrapped_missing["check_id"]

        offline = _record(client_db, client_id)
        offline_result = routes._verify_and_record(offline, actor_label="Test")
        assert offline_result["offline"] is True
        assert offline_result["verified"] is False
        stored_offline = client_db.get_eligibility_one(offline["id"])
        assert stored_offline["VerifiedBy"] == ""
        assert stored_offline["EligibilityState"]["APPROVE"]["approved"] is False
        assert client_db.has_real_eligibility_evidence(offline["id"]) is False
        assert client_db.get_eligibility_checks(offline["id"])[0]["engine_state"]

        from eligibility_hybrid import Benefit, CoverageResult, CoverageStatus

        class FakeLiveProvider:
            configured = True
            name = "stedi"

            def verify(self, req):
                return CoverageResult(
                    status=CoverageStatus.ACTIVE,
                    source="stedi",
                    payer_name=req.payer_name,
                    member_id=req.member_id or "",
                    benefit=Benefit(copay=20, deductible_total=500),
                    prior_auth_required=True,
                    raw={"request_json": "{}", "x12_271": "ISA*TEST~"},
                )

        routes._build_live_eligibility_provider = lambda payer_name="": FakeLiveProvider()
        live = _record(client_db, client_id)
        live_result = routes._verify_and_record(
            live, actor_label="Manual", actor_username="verifier", mark_completed=True
        )
        stored_live = client_db.get_eligibility_one(live["id"])
        assert live_result["status"] == "Active"
        assert stored_live["BillingReadiness"] == "On Hold"
        assert stored_live["Stage"] == "Completed"
        assert client_db.has_real_eligibility_evidence(live["id"]) is True
        assert stored_live["EligibilityState"]["APPROVE"]["approved"] is False
        assert stored_live["EligibilityState"]["ELIGIBILITY_ENGINE"]["decisions"]["lifecycle_status"] == "POLICY_HOLD"

        routes._require_user = lambda _session: {
            "id": client_id, "role": "client", "username": "elig-test",
        }
        missing_body = routes.EligibilityCheckIn(
            patient={
                "first_name": "Example", "last_name": "Patient",
                "dob": "1980-01-01", "member_id": "MEMBER1234",
            },
            payer={"payer_name": ""},
        )
        missing_api = routes.eligibility_check(missing_body, None)
        missing_payload = json.loads(missing_api.body)
        assert missing_api.status_code == 400
        assert missing_payload["missing_fields"] == ["payer_name"]
        assert missing_payload["engine_state"]["OPS"]["status"] == "blocked"

        class UnconfiguredProvider:
            configured = False
            name = "stedi"

        complete_body = routes.EligibilityCheckIn(
            patient={
                "first_name": "Example", "last_name": "Patient",
                "dob": "1980-01-01", "member_id": "MEMBER1234",
            },
            payer={"payer_name": "Aetna"},
        )
        routes._build_live_eligibility_provider = lambda payer_name="": UnconfiguredProvider()
        disconnected_api = routes.eligibility_check(complete_body, None)
        assert disconnected_api["configured"] is False
        assert disconnected_api["check_id"]
        assert disconnected_api["engine_state"]["ELIGIBILITY_ENGINE"]["decisions"]["lifecycle_status"] == "PAYER_CONNECTION_REQUIRED"

        routes._build_live_eligibility_provider = lambda payer_name="": FakeLiveProvider()
        live_api = routes.eligibility_check(complete_body, None)
        assert live_api["eligibility_status"] == "ACTIVE"
        assert live_api["check_id"]
        assert live_api["engine_state"]["APPROVE"]["approved"] is False

        universal_body = routes.UniversalEligibilityIn(
            patient={"id": "patient-1"},
            insurance={
                "active": True,
                "payer_name": "Aetna",
                "plan_type": "PPO",
                "covered_specialties": ["cardiology"],
                "excluded_cpt": [],
                "prior_auth_cpt": [],
                "excluded_icd": [],
            },
            provider={"in_network": True, "specialty": "cardiology"},
            visit={"cpt_codes": ["99213"], "icd10_codes": ["I10"]},
            eligibility_id=offline["id"],
            client_id=client_id,
        )
        universal_result = routes.universal_eligibility_eval(universal_body, None)
        assert universal_result["decision"] == "APPROVED"
        assert universal_result["billing_ready"] is True
        universal_check = client_db.get_eligibility_check_raw(
            universal_result["check_id"]
        )
        assert universal_check["source"] == "universal-rules"
        assert "patient-1" not in (universal_check["result_json"] or "")

        client_db.save_eligibility_payer_rule({
            "rule_key": "facility-aetna-87798-hold",
            "client_id": client_id,
            "payer_pattern": "aetna",
            "cpt_code": "87798",
            "criteria": {},
            "decision": "NOT_ELIGIBLE",
            "reason": "Facility contract excludes this service.",
            "actions": ["Obtain payer exception before service"],
            "source": "Facility contract amendment 2026-07",
            "version": "2026-07",
        }, updated_by="admin")
        facility_check = routes._eligibility_policy_checks(
            ["87798"], "Aetna", "87798 N39.0", "Active",
            client_id=client_id,
        )[0]
        assert facility_check["coverage_status"] == "NOT_ELIGIBLE"
        assert facility_check["payer_rules"][0]["rule_key"] == "facility-aetna-87798-hold"
        other_facility_check = routes._eligibility_policy_checks(
            ["87798"], "Aetna", "87798 N39.0", "Active",
            client_id=client_id + 999,
        )[0]
        assert other_facility_check["coverage_status"] == "APPROVED"
        assert other_facility_check["payer_rules"] == []

        affected_record = _record(
            client_db, client_id, RequestedServices="87798 N39.0"
        )
        routes._require_full_admin = lambda _session: {
            "id": 1, "role": "admin", "username": "admin",
        }
        rule_body = routes.EligibilityPayerRuleIn(
            rule_key="facility-aetna-87798-pa-api",
            client_id=client_id,
            payer_pattern="aetna",
            cpt_code="87798",
            criteria={},
            decision="PA_REQUIRED",
            reason="Facility policy requires PA.",
            actions=["Submit prior authorization"],
            source="Facility payer contract 2026-07",
            version="2026-07",
        )
        saved = routes.admin_save_eligibility_rule(rule_body, None)
        assert saved["ok"] is True
        assert saved["affected_records"] >= 1
        held = client_db.get_eligibility_one(affected_record["id"])
        assert held["BillingReadiness"] == "On Hold"
        assert held["Stage"] == "In Progress"
        listed = routes.admin_list_eligibility_rules(client_id, False, None)
        assert any(rule["id"] == saved["rule"]["id"] for rule in listed["rules"])
        tracker = routes.admin_eligibility_engine_tracker(None)
        assert tracker["OPS"]["records_evaluated"] >= 1
        assert "Eligibility Test" in tracker["TRACK"]["by_facility"]
        assert "Aetna" in tracker["TRACK"]["by_payer"]
        assert "87798" in tracker["TRACK"]["by_product"]
        deactivated = routes.admin_deactivate_eligibility_rule(
            saved["rule"]["id"], None
        )
        assert deactivated["ok"] is True
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    test_live_source_requires_sendable_provider_identity()
    test_missing_input_and_offline_and_live_hold()
    print("2 eligibility lifecycle integration tests passed")