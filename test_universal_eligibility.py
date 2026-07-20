"""Behavior tests for the universal eligibility and billing evaluator."""
from eligibility_hybrid import universal_eligibility_engine


PATIENT = {"id": "patient-1"}
INSURANCE = {
    "active": True,
    "plan_type": "PPO",
    "covered_specialties": ["cardiology", "primary care"],
    "excluded_cpt": [],
    "prior_auth_cpt": [],
    "excluded_icd": [],
}
PROVIDER = {"in_network": True, "specialty": "Cardiology"}
VISIT = {"cpt_codes": ["99213"], "icd10_codes": ["I10"]}


def _evaluate(insurance=None, provider=None, visit=None):
    return universal_eligibility_engine(
        PATIENT,
        insurance if insurance is not None else dict(INSURANCE),
        provider if provider is not None else dict(PROVIDER),
        visit if visit is not None else dict(VISIT),
    )


def test_clean_ppo_visit_is_approved():
    result = _evaluate()
    assert result["eligible"] is True
    assert result["billing_ready"] is True
    assert result["decision"] == "APPROVED"
    assert result["covered_services"] == ["99213"]


def test_inactive_plan_is_not_eligible():
    result = _evaluate(insurance={**INSURANCE, "active": False})
    assert result["eligible"] is False
    assert result["reason"] == "Insurance inactive"


def test_hmo_out_of_network_is_not_allowed():
    result = _evaluate(
        insurance={**INSURANCE, "plan_type": "HMO"},
        provider={**PROVIDER, "in_network": False},
    )
    assert result["decision"] == "NOT_ELIGIBLE"
    assert result["requires_referral"] is True
    assert result["network_status"] == "Out-of-Network"


def test_ppo_out_of_network_is_reported_but_not_automatically_denied():
    result = _evaluate(provider={**PROVIDER, "in_network": False})
    assert result["eligible"] is True
    assert result["billing_ready"] is True
    assert result["network_status"] == "Out-of-Network"


def test_in_network_hmo_without_referral_is_held():
    result = _evaluate(insurance={**INSURANCE, "plan_type": "HMO"})
    assert result["eligible"] is True
    assert result["billing_ready"] is False
    assert result["decision"] == "HOLD"
    assert result["requires_referral"] is True
    assert {item["code"] for item in result["next_actions"]} == {"OBTAIN_REFERRAL"}


def test_specialty_exclusion_stops_billing():
    result = _evaluate(provider={**PROVIDER, "specialty": "Dermatology"})
    assert result["eligible"] is False
    assert result["reason"] == "Dermatology not covered by plan"


def test_prior_auth_and_noncovered_lines_create_hold():
    result = _evaluate(
        insurance={
            **INSURANCE,
            "excluded_cpt": ["11111"],
            "prior_auth_cpt": ["22222"],
        },
        visit={"cpt_codes": ["11111", "22222", "33333"], "icd10_codes": ["I10"]},
    )
    assert result["eligible"] is True
    assert result["billing_ready"] is False
    assert result["decision"] == "HOLD"
    assert result["covered_services"] == ["22222", "33333"]
    assert result["non_covered_services"] == ["11111"]
    assert result["prior_auth_services"] == ["22222"]


def test_referral_and_prior_auth_evidence_release_hmo_visit():
    result = _evaluate(
        insurance={**INSURANCE, "plan_type": "HMO", "prior_auth_cpt": ["99213"]},
        visit={
            "cpt_codes": ["99213"],
            "icd10_codes": ["I10"],
            "referral_on_file": True,
            "authorized_cpt": ["99213"],
        },
    )
    assert result["eligible"] is True
    assert result["billing_ready"] is True
    assert result["requires_referral"] is True
    assert result["requires_prior_auth"] is True


def test_excluded_diagnosis_is_not_eligible():
    result = _evaluate(insurance={**INSURANCE, "excluded_icd": ["I10"]})
    assert result["decision"] == "NOT_ELIGIBLE"
    assert result["reason"] == "Diagnosis not covered"


def test_all_services_excluded_returns_no_covered_services():
    result = _evaluate(
        insurance={**INSURANCE, "excluded_cpt": ["99213"]},
    )
    assert result["eligible"] is False
    assert result["decision"] == "NOT_ELIGIBLE"
    assert result["reason"] == "No covered services"


def test_missing_payer_facts_are_requested_not_invented():
    insurance = dict(INSURANCE)
    del insurance["active"]
    del insurance["prior_auth_cpt"]
    result = _evaluate(insurance=insurance)
    assert result["decision"] == "INPUT_REQUIRED"
    assert result["missing_fields"] == [
        "insurance.active", "insurance.prior_auth_cpt",
    ]
    assert result["reason"] == "Missing required eligibility data"


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items())
             if name.startswith("test_") and callable(value)]
    for test in tests:
        test()
    print(f"{len(tests)} universal eligibility tests passed")