"""Universal deterministic eligibility and billing evaluation.

This module consumes verified insurance facts and sourced plan rules. It does
not call a payer and never treats missing data as inactive, covered, or exempt.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_list(value: Any, *, upper: bool = False) -> list[str] | None:
    if not isinstance(value, (list, tuple, set)):
        return None
    output: list[str] = []
    for item in value:
        normalized = _text(item)
        normalized = normalized.upper() if upper else normalized.lower()
        if normalized and normalized not in output:
            output.append(normalized)
    return output


def universal_eligibility_engine(
    patient: dict,
    insurance: dict,
    provider: dict,
    visit: dict,
) -> dict:
    """Evaluate eligibility and billing requirements for any specialty.

    ``eligible`` means at least one ordered service is covered by the supplied
    plan facts. ``billing_ready`` is stricter: every ordered service must be
    covered and all required referral/prior-authorization evidence must be on
    file. The caller must supply real payer facts; missing facts create an
    INPUT_REQUIRED hold instead of an inferred determination.
    """
    result = {
        "eligible": False,
        "billing_ready": False,
        "decision": "INPUT_REQUIRED",
        "reason": "",
        "network_status": "Unknown",
        "covered_services": [],
        "non_covered_services": [],
        "requires_referral": False,
        "requires_prior_auth": False,
        "prior_auth_services": [],
        "missing_fields": [],
        "checks": [],
        "next_actions": [],
    }

    inputs = {
        "patient": patient,
        "insurance": insurance,
        "provider": provider,
        "visit": visit,
    }
    invalid_objects = [name for name, value in inputs.items() if not isinstance(value, Mapping)]
    if invalid_objects:
        result["missing_fields"] = invalid_objects
        result["reason"] = "Patient, insurance, provider, and visit must be objects"
        result["next_actions"] = [
            {"code": "PROVIDE_OBJECT", "field": field} for field in invalid_objects
        ]
        return result

    required_scalars = (
        ("insurance.active", insurance, "active"),
        ("insurance.plan_type", insurance, "plan_type"),
        ("provider.in_network", provider, "in_network"),
        ("provider.specialty", provider, "specialty"),
    )
    for path, source, key in required_scalars:
        if key not in source or source.get(key) in (None, ""):
            result["missing_fields"].append(path)

    if "active" in insurance and not isinstance(insurance.get("active"), bool):
        result["missing_fields"].append("insurance.active(boolean)")
    if "in_network" in provider and not isinstance(provider.get("in_network"), bool):
        result["missing_fields"].append("provider.in_network(boolean)")

    rule_lists = {}
    for key in ("covered_specialties", "excluded_cpt", "prior_auth_cpt", "excluded_icd"):
        normalized = _normalized_list(
            insurance.get(key), upper=key in ("excluded_cpt", "prior_auth_cpt", "excluded_icd")
        )
        if normalized is None:
            result["missing_fields"].append(f"insurance.{key}")
        else:
            rule_lists[key] = normalized

    cpt_codes = _normalized_list(visit.get("cpt_codes"), upper=True)
    if cpt_codes is None or not cpt_codes:
        result["missing_fields"].append("visit.cpt_codes")
        cpt_codes = []
    icd10_codes = _normalized_list(visit.get("icd10_codes", []), upper=True)
    if icd10_codes is None:
        result["missing_fields"].append("visit.icd10_codes")
        icd10_codes = []

    if result["missing_fields"]:
        result["missing_fields"] = list(dict.fromkeys(result["missing_fields"]))
        result["reason"] = "Missing required eligibility data"
        result["checks"].append({
            "code": "INPUT_VALIDATION",
            "status": "FAIL",
            "missing_fields": result["missing_fields"],
        })
        result["next_actions"] = [
            {"code": "PROVIDE_REQUIRED_FIELD", "field": field}
            for field in result["missing_fields"]
        ]
        return result

    # 1. Insurance active?
    active = insurance["active"]
    result["checks"].append({
        "code": "INSURANCE_ACTIVE", "status": "PASS" if active else "FAIL"
    })
    if not active:
        result["decision"] = "NOT_ELIGIBLE"
        result["reason"] = "Insurance inactive"
        result["next_actions"].append({"code": "REVERIFY_OR_COLLECT_ALTERNATE_INSURANCE"})
        return result

    # 2. Provider in-network?
    in_network = provider["in_network"]
    result["network_status"] = "In-Network" if in_network else "Out-of-Network"
    result["checks"].append({
        "code": "NETWORK_STATUS",
        "status": "PASS" if in_network else "OUT_OF_NETWORK",
        "value": result["network_status"],
    })

    # 3. Plan type rules.
    plan_type = _text(insurance["plan_type"]).upper()
    result["requires_referral"] = plan_type == "HMO"
    referral_on_file = visit.get("referral_on_file") is True
    if plan_type in ("EPO", "HMO") and not in_network:
        result["decision"] = "NOT_ELIGIBLE"
        result["reason"] = "Out-of-network not allowed for this plan"
        result["checks"].append({"code": "PLAN_NETWORK_RULE", "status": "FAIL"})
        result["next_actions"].append({"code": "ROUTE_TO_IN_NETWORK_PROVIDER"})
        return result
    result["checks"].append({
        "code": "PLAN_NETWORK_RULE",
        "status": "PASS",
        "plan_type": plan_type,
    })

    # 4. Specialty coverage.
    specialty = _text(provider["specialty"]).lower()
    covered_specialties = rule_lists["covered_specialties"]
    if covered_specialties and specialty not in covered_specialties:
        result["decision"] = "NOT_ELIGIBLE"
        result["reason"] = f"{specialty.capitalize()} not covered by plan"
        result["checks"].append({"code": "SPECIALTY_COVERAGE", "status": "FAIL"})
        result["next_actions"].append({"code": "CONFIRM_SPECIALTY_BENEFIT_WITH_PAYER"})
        return result
    result["checks"].append({"code": "SPECIALTY_COVERAGE", "status": "PASS"})

    # 5. CPT coverage and prior authorization.
    excluded_cpt = set(rule_lists["excluded_cpt"])
    prior_auth_cpt = set(rule_lists["prior_auth_cpt"])
    for code in cpt_codes:
        if code in excluded_cpt:
            result["non_covered_services"].append(code)
            result["checks"].append({
                "code": "CPT_COVERAGE", "cpt": code, "status": "NOT_COVERED"
            })
        else:
            result["covered_services"].append(code)
            if code in prior_auth_cpt:
                result["requires_prior_auth"] = True
                result["prior_auth_services"].append(code)
                status = "PRIOR_AUTH_REQUIRED"
            else:
                status = "COVERED"
            result["checks"].append({
                "code": "CPT_COVERAGE", "cpt": code, "status": status
            })

    # 6. ICD-10 exclusions.
    excluded_icd = set(rule_lists["excluded_icd"])
    denied_diagnoses = [code for code in icd10_codes if code in excluded_icd]
    if denied_diagnoses:
        result["decision"] = "NOT_ELIGIBLE"
        result["reason"] = "Diagnosis not covered"
        result["checks"].append({
            "code": "ICD10_COVERAGE",
            "status": "FAIL",
            "excluded_diagnoses": denied_diagnoses,
        })
        result["next_actions"].append({"code": "REVIEW_DIAGNOSIS_COVERAGE"})
        return result
    result["checks"].append({"code": "ICD10_COVERAGE", "status": "PASS"})

    # 7. Final determination.
    if not result["covered_services"]:
        result["decision"] = "NOT_ELIGIBLE"
        result["reason"] = "No covered services"
        result["next_actions"].append({"code": "REMOVE_OR_REPLACE_NON_COVERED_SERVICES"})
        return result

    result["eligible"] = True
    authorized_cpt = set(_normalized_list(visit.get("authorized_cpt", []), upper=True) or [])
    auth_missing = [code for code in result["prior_auth_services"] if code not in authorized_cpt]
    holds = []
    if result["requires_referral"] and not referral_on_file:
        holds.append("referral")
        result["next_actions"].append({"code": "OBTAIN_REFERRAL"})
    if auth_missing:
        holds.append("prior authorization")
        result["next_actions"].append({
            "code": "OBTAIN_PRIOR_AUTH",
            "cpt_codes": auth_missing,
        })
    if result["non_covered_services"]:
        holds.append("non-covered services")
        result["next_actions"].append({
            "code": "REMOVE_OR_REPLACE_NON_COVERED_SERVICES",
            "cpt_codes": result["non_covered_services"],
        })

    if holds:
        result["decision"] = "HOLD"
        result["reason"] = "Eligible with unresolved " + ", ".join(holds)
    else:
        result["billing_ready"] = True
        result["decision"] = "APPROVED"
        result["reason"] = "Eligible for billing"
    return result