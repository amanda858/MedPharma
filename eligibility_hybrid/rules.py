"""Versioned deterministic payer-rule matching for eligibility decisions."""
from __future__ import annotations

from datetime import date
from typing import Any


ALLOWED_DECISIONS = {
    "APPROVED", "PA_REQUIRED", "MEDICAL_NECESSITY_HOLD",
    "DENY_RISK", "NOT_ELIGIBLE", "INFO",
}
ALLOWED_CRITERIA = {
    "required_fields", "icd10_any", "member_prefixes", "states",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _contains(pattern: str, value: str) -> bool:
    pattern = _text(pattern).lower()
    if not pattern or pattern == "*":
        return True
    return pattern in _text(value).lower()


def _date_in_range(dos: str, effective_date: str, term_date: str) -> bool:
    try:
        target = date.fromisoformat(_text(dos))
    except ValueError:
        return not effective_date and not term_date
    try:
        if effective_date and target < date.fromisoformat(_text(effective_date)):
            return False
        if term_date and target > date.fromisoformat(_text(term_date)):
            return False
    except ValueError:
        return False
    return True


def _specificity(rule: dict) -> int:
    return (
        (8 if rule.get("client_id") else 0)
        + (4 if _text(rule.get("cpt_code")) not in ("", "*") else 0)
        + (2 if _text(rule.get("plan_pattern")) not in ("", "*") else 0)
        + (1 if _text(rule.get("payer_pattern")) not in ("", "*") else 0)
    )


def evaluate_eligibility_rules(rules: list[dict], context: dict) -> dict:
    """Evaluate explicit rules only; unknown payer requirements stay unknown."""
    matches: list[dict] = []
    errors: list[dict] = []
    ordered = sorted(
        (dict(rule) for rule in (rules or [])),
        key=lambda rule: (-_specificity(rule), _text(rule.get("rule_key")), int(rule.get("id") or 0)),
    )
    context_client = int(context.get("client_id") or 0)
    context_cpt = _text(context.get("cpt_code"))
    context_fields = dict(context.get("fields") or {})
    context_icd10 = {_text(code).upper() for code in (context.get("icd10_codes") or [])}
    member_id = _text(context.get("member_id")).upper()
    state = _text(context.get("state")).upper()

    for rule in ordered:
        rule_key = _text(rule.get("rule_key")) or f"rule-{rule.get('id') or 'unknown'}"
        if not bool(rule.get("is_active", True)):
            continue
        source = _text(rule.get("source"))
        decision = _text(rule.get("decision")).upper()
        criteria = rule.get("criteria") or {}
        if not isinstance(criteria, dict):
            errors.append({"rule_key": rule_key, "code": "INVALID_CRITERIA"})
            continue
        unknown_keys = sorted(set(criteria) - ALLOWED_CRITERIA)
        if unknown_keys:
            errors.append({
                "rule_key": rule_key,
                "code": "UNKNOWN_CRITERIA",
                "fields": unknown_keys,
            })
            continue
        if not source:
            errors.append({"rule_key": rule_key, "code": "MISSING_SOURCE"})
            continue
        if decision not in ALLOWED_DECISIONS:
            errors.append({"rule_key": rule_key, "code": "INVALID_DECISION"})
            continue
        rule_client = int(rule.get("client_id") or 0)
        if rule_client and rule_client != context_client:
            continue
        if not _contains(rule.get("payer_pattern"), context.get("payer_name")):
            continue
        if not _contains(rule.get("plan_pattern"), context.get("plan_name")):
            continue
        rule_cpt = _text(rule.get("cpt_code"))
        if rule_cpt not in ("", "*") and rule_cpt != context_cpt:
            continue
        if not _date_in_range(
            context.get("date_of_service") or "",
            rule.get("effective_date") or "",
            rule.get("term_date") or "",
        ):
            continue

        required = [_text(field) for field in criteria.get("required_fields") or []]
        missing = [field for field in required if not _text(context_fields.get(field))]
        icd10_any = {_text(code).upper() for code in criteria.get("icd10_any") or []}
        prefixes = [_text(prefix).upper() for prefix in criteria.get("member_prefixes") or []]
        states = {_text(item).upper() for item in criteria.get("states") or []}
        if icd10_any and not (context_icd10 & icd10_any):
            continue
        if prefixes and not any(member_id.startswith(prefix) for prefix in prefixes):
            continue
        if states and state not in states:
            continue

        matches.append({
            "id": rule.get("id"),
            "rule_key": rule_key,
            "decision": decision,
            "reason": _text(rule.get("reason")),
            "actions": [_text(item) for item in (rule.get("actions") or []) if _text(item)],
            "source": source,
            "version": _text(rule.get("version")) or "1",
            "specificity": _specificity(rule),
            "missing_fields": missing,
        })

    return {
        "matches": matches,
        "requirements_known": bool(matches),
        "unknown_requirements": [] if matches else ["payer_specific_rules"],
        "errors": errors,
    }