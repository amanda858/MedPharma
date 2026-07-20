"""Focused deterministic tests for facility/payer/product eligibility rules."""
from eligibility_hybrid.rules import evaluate_eligibility_rules


CONTEXT = {
    "client_id": 43,
    "payer_name": "Aetna Medicare PPO",
    "plan_name": "Gold PPO",
    "cpt_code": "87631",
    "date_of_service": "2026-07-18",
    "icd10_codes": ["J12.81"],
    "member_id": "ABC1234",
    "state": "FL",
    "fields": {"ordering_provider_npi": "1234567893"},
}


def _rule(**overrides):
    rule = {
        "id": 1,
        "rule_key": "aetna-87631-pa",
        "client_id": None,
        "payer_pattern": "aetna",
        "plan_pattern": "ppo",
        "cpt_code": "87631",
        "criteria": {"icd10_any": ["J12.81"]},
        "decision": "PA_REQUIRED",
        "reason": "Current payer policy requires prior authorization.",
        "actions": ["Submit prior authorization"],
        "source": "Aetna policy bulletin 123",
        "version": "2026-07",
        "effective_date": "2026-01-01",
        "term_date": "",
        "is_active": True,
    }
    rule.update(overrides)
    return rule


def test_specific_rule_matches_with_provenance():
    result = evaluate_eligibility_rules([_rule()], CONTEXT)
    assert result["requirements_known"] is True
    assert result["matches"][0]["decision"] == "PA_REQUIRED"
    assert result["matches"][0]["source"] == "Aetna policy bulletin 123"


def test_facility_scope_and_dates_are_enforced():
    assert not evaluate_eligibility_rules([_rule(client_id=99)], CONTEXT)["matches"]
    assert not evaluate_eligibility_rules(
        [_rule(effective_date="2027-01-01")], CONTEXT
    )["matches"]


def test_unknown_rules_remain_unknown():
    result = evaluate_eligibility_rules([], CONTEXT)
    assert result["requirements_known"] is False
    assert result["unknown_requirements"] == ["payer_specific_rules"]


def test_missing_source_and_unknown_criteria_are_errors():
    result = evaluate_eligibility_rules([
        _rule(rule_key="missing-source", source=""),
        _rule(rule_key="unknown-criteria", criteria={"free_text_python": "x"}),
    ], CONTEXT)
    assert {item["code"] for item in result["errors"]} == {
        "MISSING_SOURCE", "UNKNOWN_CRITERIA",
    }


def test_more_specific_rule_is_evaluated_first():
    broad = _rule(id=2, rule_key="broad", client_id=None, cpt_code="*")
    facility = _rule(id=3, rule_key="facility", client_id=43)
    result = evaluate_eligibility_rules([broad, facility], CONTEXT)
    assert [item["rule_key"] for item in result["matches"]] == ["facility", "broad"]


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items())
             if name.startswith("test_") and callable(value)]
    for test in tests:
        test()
    print(f"{len(tests)} eligibility rule tests passed")