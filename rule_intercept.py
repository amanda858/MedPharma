"""
rule_intercept.py

A simple rule engine that:
- Detects credentialing, billing, and workflow support requests
- Extracts medical specialty
- Routes to the correct handler
- Returns a structured, trackable result
"""

from __future__ import annotations

import re
import time
from typing import Optional

# =========================
# RULE DEFINITIONS
# =========================

KEYWORD_MAP = {
    "credentialing": ["credentialing", "enrollment", "provider enrollment", "caqh"],
    "billing": ["billing", "claims", "revenue cycle", "rcm"],
    "workflow": ["workflow", "operations", "process", "support"],
}

SPECIALTY_PATTERNS = [
    r"cardiology",
    r"orthopedics?",
    r"behavioral health",
    r"psychiatry",
    r"primary care",
    r"family medicine",
    r"internal medicine",
    r"pediatrics?",
    r"dermatology",
    r"neurology",
    r"oncology",
    r"obgyn|ob/gyn|ob\-gyn",
    r"urgent care",
    r"endocrinology",
    r"gastroenterology",
    r"nephrology",
    r"urology",
    r"rheumatology",
]

# =========================
# CORE INTERCEPT LOGIC
# =========================


def detect_category(text: str) -> Optional[str]:
    lowered = text.lower()
    for category, keywords in KEYWORD_MAP.items():
        if any(k.lower() in lowered for k in keywords):
            return category
    return None


def detect_specialty(text: str) -> Optional[str]:
    lowered = text.lower()
    for pattern in SPECIALTY_PATTERNS:
        match = re.search(pattern, lowered)
        if match:
            return match.group(0)
    return None


# =========================
# ROUTING HANDLERS
# =========================


def handle_credentialing(specialty: Optional[str]) -> dict:
    return {
        "category": "credentialing",
        "specialty": specialty or "unspecified",
        "required_contacts": [
            "Provider Enrollment Lead",
            "CAQH Administrator",
            "Payer Enrollment Coordinator",
        ],
        "notes": "Verify CAQH, NPI, taxonomy, state Medicaid enrollment, and payer-specific rosters.",
    }



def handle_billing(specialty: Optional[str]) -> dict:
    return {
        "category": "billing",
        "specialty": specialty or "unspecified",
        "required_contacts": [
            "Revenue Cycle Manager",
            "Claims Submission Lead",
            "EDI/ERA Enrollment Specialist",
        ],
        "notes": "Check payer billing manuals, prior auth rules, and claim format requirements.",
    }



def handle_workflow(specialty: Optional[str]) -> dict:
    return {
        "category": "workflow",
        "specialty": specialty or "unspecified",
        "required_contacts": [
            "Clinical Operations Lead",
            "Practice Manager",
            "Quality/Compliance Coordinator",
        ],
        "notes": "Map intake -> scheduling -> documentation -> billing -> reporting.",
    }


ROUTE_MAP = {
    "credentialing": handle_credentialing,
    "billing": handle_billing,
    "workflow": handle_workflow,
}


# =========================
# EXCEL UPLOAD INTERCEPT
# =========================

EXCEL_CATEGORY_SYNONYMS = {
    "Claims": [
        "claim", "patient", "dos", "date of service", "cpt", "hcpcs", "charge",
        "allowed", "paid", "balance", "denial", "remit", "eob", "claim status",
    ],
    "Credentialing": [
        "credential", "recredential", "caqh", "expiration", "approved", "credentialing",
        "provider enrollment", "taxonomy", "npi",
    ],
    "Enrollment": [
        "enrollment", "enroll", "effective", "in network", "in-network", "par",
        "payer enrollment", "participation", "contracted",
    ],
    "EDI": [
        "edi", "era", "eft", "clearinghouse", "trading partner", "submitter",
        "receiver", "payer id", "835", "837", "x12",
    ],
}


def _norm_token(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def intercept_excel_upload(headers: list[str] | None = None, filename: str = "", description: str = "") -> dict:
    """Deterministically classify an Excel upload category.

    Returns:
        {
          "category": "Claims|Credentialing|Enrollment|EDI|None",
          "confidence": float,
          "scores": { ... },
          "matched_terms": {category: [terms...]},
          "method": "rule-intercept"
        }
    """
    headers = headers or []
    blob = " ".join([*(headers or []), filename or "", description or ""]).lower()

    scores: dict[str, int] = {k: 0 for k in EXCEL_CATEGORY_SYNONYMS}
    matched_terms: dict[str, list[str]] = {k: [] for k in EXCEL_CATEGORY_SYNONYMS}

    # Priority rules for explicit markers.
    if re.search(r"\b(837|835|era|eft|edi)\b", blob):
        scores["EDI"] += 6
    if re.search(r"\b(cpt|hcpcs|dos|claim)\b", blob):
        scores["Claims"] += 5
    if re.search(r"\b(caqh|credential|recredential)\b", blob):
        scores["Credentialing"] += 5
    if re.search(r"\b(enrollment|effective date|in network|in-network|par)\b", blob):
        scores["Enrollment"] += 5

    # General keyword scoring.
    for category, terms in EXCEL_CATEGORY_SYNONYMS.items():
        for term in terms:
            t = _norm_token(term)
            if t and t in blob:
                scores[category] += 2 if len(t) >= 8 else 1
                matched_terms[category].append(term)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_cat, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0

    category = best_cat if best_score >= 3 and (best_score - second_score) >= 1 else None
    confidence = 0.0
    if best_score > 0:
        confidence = round(best_score / max(best_score + second_score, 1), 3)

    return {
        "category": category,
        "confidence": confidence,
        "scores": scores,
        "matched_terms": matched_terms,
        "method": "rule-intercept",
    }


# =========================
# PUBLIC API
# =========================


def intercept_request(text: str) -> dict:
    category = detect_category(text)
    specialty = detect_specialty(text)

    result = {
        "timestamp": time.time(),
        "input": text,
        "category_detected": category,
        "specialty_detected": specialty,
    }

    if category:
        handler = ROUTE_MAP[category]
        result["routed_to"] = handler.__name__
        result["result"] = handler(specialty)
    else:
        result["routed_to"] = None
        result["result"] = {
            "category": None,
            "specialty": specialty,
            "notes": "No matching rule found.",
        }

    return result
