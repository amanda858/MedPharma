"""Deterministic five-stage state contract for MedPharma eligibility.

The contract is vendor-neutral, JSON-serializable, and safe to persist. It
does not decide payer facts; callers supply results from real payer evidence
and deterministic policy checks.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


TOP_LEVEL_KEYS = (
    "OPS", "TRACK", "COMMUNICATE", "APPROVE", "EXECUTE",
    "ELIGIBILITY_ENGINE", "ERRORS",
)
REQUIRED_INPUT_FIELDS = ("patient_name", "dob", "payer_name", "member_id")
BLOCKING_POLICY_STATUSES = {
    "MEDICAL_NECESSITY_HOLD", "DENY_RISK", "NOT_ELIGIBLE", "PA_REQUIRED",
    "SERVICE_INPUT_REQUIRED",
}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _masked_member_id(value: str) -> dict:
    member_id = _clean(value)
    digest = hashlib.sha256(member_id.encode("utf-8")).hexdigest() if member_id else ""
    return {
        "present": bool(member_id),
        "masked": ("*" * max(0, len(member_id) - 4) + member_id[-4:]) if member_id else "",
        "sha256": digest,
    }


def _error_items(errors: Any) -> list[str]:
    if not errors:
        return []
    if isinstance(errors, str):
        return [errors] if errors.strip() else []
    return [str(item) for item in errors if str(item).strip()]


def derive_billing_readiness(
    coverage_status: str,
    *,
    requested_services: list[str] | None = None,
    policy_checks: list[dict] | None = None,
    prior_auth_required: bool | None = None,
) -> str:
    """Return the only safe billing-readiness value for current evidence."""
    status = _clean(coverage_status).title()
    if status in ("Inactive", "Termed"):
        return "Not Billable"
    if status != "Active":
        return ""
    if not requested_services:
        return "On Hold"
    if prior_auth_required is True:
        return "On Hold"
    if any(item.get("unknown_requirements") for item in (policy_checks or [])):
        return "On Hold"
    if any(
        _clean(item.get("coverage_status")).upper() in BLOCKING_POLICY_STATUSES
        for item in (policy_checks or [])
    ):
        return "On Hold"
    return "Clear to Bill"


def build_eligibility_lifecycle(
    inputs: dict,
    *,
    source: str = "",
    configured: bool = False,
    verified: bool = False,
    coverage_status: str = "Unknown",
    policy_checks: list[dict] | None = None,
    billing_readiness: str = "",
    record_stage: str = "Received",
    summary: str = "",
    check_id: int | None = None,
    actor: str = "",
    changed: bool = False,
    errors: Any = None,
    evaluated_at: str | None = None,
) -> dict:
    """Build the canonical OPS->TRACK->COMMUNICATE->APPROVE->EXECUTE state.

    Approval is intentionally strict: complete core inputs, a configured live
    provider, immutable payer evidence, active coverage, and no blocking policy
    result are all required. Offline policy review can guide work but can never
    represent payer verification or approval.
    """
    inputs = dict(inputs or {})
    policy_checks = [dict(item) for item in (policy_checks or [])]
    error_items = _error_items(errors)
    missing_fields = [field for field in REQUIRED_INPUT_FIELDS if not _clean(inputs.get(field))]
    blocking_checks = [
        item for item in policy_checks
        if _clean(item.get("coverage_status")).upper() in BLOCKING_POLICY_STATUSES
    ]
    unknown_requirements = sorted({
        _clean(requirement)
        for item in policy_checks
        for requirement in (item.get("unknown_requirements") or [])
        if _clean(requirement)
    })
    status_normalized = _clean(coverage_status).title() or "Unknown"
    readiness_normalized = _clean(billing_readiness)
    has_evidence = bool(configured and verified and check_id)
    approved = bool(
        not missing_fields
        and not error_items
        and has_evidence
        and status_normalized == "Active"
        and not blocking_checks
        and not unknown_requirements
        and readiness_normalized == "Clear to Bill"
    )

    if missing_fields:
        lifecycle_status = "INPUT_REQUIRED"
    elif error_items:
        lifecycle_status = "ERROR"
    elif not configured:
        lifecycle_status = "PAYER_CONNECTION_REQUIRED"
    elif not has_evidence:
        lifecycle_status = "PAYER_RESPONSE_REQUIRED"
    elif status_normalized in ("Inactive", "Termed"):
        lifecycle_status = "NOT_ELIGIBLE"
    elif status_normalized != "Active":
        lifecycle_status = "REVERIFY"
    elif blocking_checks:
        lifecycle_status = "POLICY_HOLD"
    elif unknown_requirements:
        lifecycle_status = "PAYER_RULES_REQUIRED"
    elif readiness_normalized != "Clear to Bill":
        lifecycle_status = "WORKFLOW_HOLD"
    else:
        lifecycle_status = "APPROVED"

    next_actions: list[dict] = []
    for field in missing_fields:
        next_actions.append({"code": "REQUEST_MISSING_FIELD", "field": field})
    for requirement in unknown_requirements:
        next_actions.append({
            "code": "REQUEST_PAYER_RULES",
            "field": requirement,
            "payer_name": _clean(inputs.get("payer_name")),
        })
    if not missing_fields and not configured:
        next_actions.append({"code": "CONNECT_PAYER_SOURCE"})
    elif configured and not has_evidence and not error_items:
        next_actions.append({"code": "RETRY_PAYER_VERIFICATION"})
    for check in blocking_checks:
        for action in check.get("actions") or []:
            next_actions.append({
                "code": "RESOLVE_POLICY_BLOCK",
                "cpt": _clean(check.get("cpt")),
                "action": _clean(action),
            })
    if status_normalized in ("Inactive", "Termed"):
        next_actions.append({"code": "CONFIRM_ALTERNATE_COVERAGE_OR_SELF_PAY"})
    elif status_normalized not in ("Active", "Inactive", "Termed") and has_evidence:
        next_actions.append({"code": "REVERIFY_AS_OF_DATE_OF_SERVICE"})
    if (status_normalized == "Active" and not blocking_checks
            and readiness_normalized != "Clear to Bill"):
        next_actions.append({"code": "RESOLVE_BILLING_HOLD"})
    if approved:
        next_actions.append({"code": "RELEASE_APPROVED_WORKFLOW"})

    evaluated_at = evaluated_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    safe_inputs = {
        "record_id": inputs.get("record_id"),
        "client_id": inputs.get("client_id"),
        "patient_name_present": bool(_clean(inputs.get("patient_name"))),
        "dob_present": bool(_clean(inputs.get("dob"))),
        "payer_name": _clean(inputs.get("payer_name")),
        "member_id": _masked_member_id(_clean(inputs.get("member_id"))),
        "requested_services": list(inputs.get("requested_services") or []),
    }
    engine = {
        "inputs": safe_inputs,
        "rules": {
            "required_inputs": list(REQUIRED_INPUT_FIELDS),
            "payer_verification": "real payer evidence required",
            "approval": "active coverage and no blocking policy checks",
            "policy_checks": policy_checks,
        },
        "checks": {
            "missing_fields": missing_fields,
            "provider_configured": bool(configured),
            "live_payer_evidence": has_evidence,
            "audit_check_id": check_id,
            "blocking_policy_checks": blocking_checks,
            "unknown_requirements": unknown_requirements,
        },
        "decisions": {
            "coverage_status": status_normalized,
            "billing_readiness": readiness_normalized,
            "approved": approved,
            "lifecycle_status": lifecycle_status,
        },
        "status": {
            "record_stage": _clean(record_stage) or "Received",
            "source": _clean(source),
            "verified": bool(verified),
            "changed": bool(changed),
            "actor": _clean(actor),
            "evaluated_at": evaluated_at,
        },
        "next_actions": {"items": next_actions},
    }
    return {
        "OPS": {
            "status": "complete" if not missing_fields else "blocked",
            "missing_fields": missing_fields,
            "provider_route": _clean(source) or "unconfigured",
        },
        "TRACK": {
            "status": "complete",
            "record_id": inputs.get("record_id"),
            "client_id": inputs.get("client_id"),
            "audit_check_id": check_id,
            "lifecycle_status": lifecycle_status,
        },
        "COMMUNICATE": {
            "status": "complete",
            "summary": _clean(summary),
            "requests": (
                [{"field": field} for field in missing_fields]
                + [{"field": requirement} for requirement in unknown_requirements]
            ),
        },
        "APPROVE": {
            "status": "approved" if approved else "not_approved",
            "approved": approved,
            "basis": "real payer evidence plus deterministic policy checks",
            "blockers": [
                _clean(item.get("coverage_status")) for item in blocking_checks
            ],
        },
        "EXECUTE": {
            "status": "complete" if approved else "blocked",
            "action": "release_approved_workflow" if approved else "hold_and_resolve",
            "next_actions": next_actions,
        },
        "ELIGIBILITY_ENGINE": engine,
        "ERRORS": {"items": error_items},
    }


def validate_lifecycle_shape(state: dict) -> bool:
    """Return True only for the canonical top-level contract."""
    return isinstance(state, dict) and tuple(state.keys()) == TOP_LEVEL_KEYS