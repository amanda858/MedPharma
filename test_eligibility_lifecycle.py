"""Focused contract tests for the persistent eligibility lifecycle state."""
from eligibility_hybrid.lifecycle import (
    TOP_LEVEL_KEYS, build_eligibility_lifecycle, derive_billing_readiness,
    validate_lifecycle_shape,
)


BASE_INPUTS = {
    "record_id": 7,
    "client_id": 43,
    "patient_name": "Example, Patient",
    "dob": "1980-01-01",
    "payer_name": "Example Payer",
    "member_id": "ABC123456",
    "requested_services": ["87631"],
}


def test_exact_top_level_contract_and_masked_member_id():
    state = build_eligibility_lifecycle(BASE_INPUTS, evaluated_at="2026-07-18T12:00:00+00:00")
    assert tuple(state.keys()) == TOP_LEVEL_KEYS
    assert validate_lifecycle_shape(state)
    member = state["ELIGIBILITY_ENGINE"]["inputs"]["member_id"]
    assert member["masked"].endswith("3456")
    assert "ABC123456" not in str(state)


def test_missing_inputs_request_only_missing_fields():
    inputs = dict(BASE_INPUTS, dob="", member_id="")
    state = build_eligibility_lifecycle(inputs, configured=True)
    assert state["OPS"]["status"] == "blocked"
    assert state["OPS"]["missing_fields"] == ["dob", "member_id"]
    assert state["COMMUNICATE"]["requests"] == [
        {"field": "dob"}, {"field": "member_id"},
    ]


def test_offline_policy_review_never_approves():
    state = build_eligibility_lifecycle(
        BASE_INPUTS, source="rule-intercept", configured=False,
        coverage_status="Unknown", changed=True,
    )
    assert state["APPROVE"]["approved"] is False
    assert state["ELIGIBILITY_ENGINE"]["decisions"]["lifecycle_status"] == "PAYER_CONNECTION_REQUIRED"


def test_live_active_clean_result_approves():
    state = build_eligibility_lifecycle(
        BASE_INPUTS, source="stedi", configured=True, verified=True,
        coverage_status="Active", check_id=99, billing_readiness="Clear to Bill",
    )
    assert state["APPROVE"]["approved"] is True
    assert state["EXECUTE"]["action"] == "release_approved_workflow"


def test_active_plan_with_prior_auth_is_held():
    state = build_eligibility_lifecycle(
        BASE_INPUTS, source="stedi", configured=True, verified=True,
        coverage_status="Active", check_id=100,
        policy_checks=[{
            "cpt": "87631", "coverage_status": "PA_REQUIRED",
            "actions": ["Route to prior_auth_queue"],
        }],
    )
    assert state["APPROVE"]["approved"] is False
    assert state["ELIGIBILITY_ENGINE"]["decisions"]["lifecycle_status"] == "POLICY_HOLD"
    assert state["EXECUTE"]["next_actions"][0]["code"] == "RESOLVE_POLICY_BLOCK"


def test_billing_readiness_requires_active_clean_service_decision():
    assert derive_billing_readiness(
        "Active", requested_services=["87631"], policy_checks=[]
    ) == "Clear to Bill"
    assert derive_billing_readiness(
        "Active", requested_services=["87631"], prior_auth_required=True
    ) == "On Hold"
    assert derive_billing_readiness("Active", requested_services=[]) == "On Hold"
    assert derive_billing_readiness("Termed", requested_services=["87631"]) == "Not Billable"
    assert derive_billing_readiness("Pending", requested_services=["87631"]) == ""


def test_active_evidence_with_workflow_hold_is_not_approved():
    state = build_eligibility_lifecycle(
        BASE_INPUTS, source="stedi", configured=True, verified=True,
        coverage_status="Active", check_id=101, billing_readiness="On Hold",
    )
    assert state["APPROVE"]["approved"] is False
    assert state["ELIGIBILITY_ENGINE"]["decisions"]["lifecycle_status"] == "WORKFLOW_HOLD"


def test_unknown_payer_rules_are_requested_without_inventing_a_decision():
    state = build_eligibility_lifecycle(
        BASE_INPUTS, source="stedi", configured=True, verified=True,
        coverage_status="Active", check_id=102, billing_readiness="Clear to Bill",
        policy_checks=[{
            "cpt": "87631", "coverage_status": "APPROVED",
            "unknown_requirements": ["payer_specific_rules"],
        }],
    )
    assert {item["field"] for item in state["COMMUNICATE"]["requests"]} == {
        "payer_specific_rules"
    }
    assert any(
        item["code"] == "REQUEST_PAYER_RULES"
        for item in state["EXECUTE"]["next_actions"]
    )
    assert state["APPROVE"]["approved"] is False
    assert state["ELIGIBILITY_ENGINE"]["decisions"]["lifecycle_status"] == "PAYER_RULES_REQUIRED"


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items())
             if name.startswith("test_") and callable(value)]
    for test in tests:
        test()
    print(f"{len(tests)} eligibility lifecycle tests passed")