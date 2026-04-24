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
# LAB LEAD INTELLIGENCE
# =========================
# Tier A = specialty / highest RCM billing complexity (tox, molecular, genomics)
# Tier B = clinical complexity (pathology, hematology, immunology, reference)
# Tier C = general / commodity / hospital outreach

_LAB_TIER_MAP: list[tuple[str, int]] = [
    # ── Tier A ──────────────────────────────────────────────────────────
    ("toxicology", 1), ("tox lab", 1), ("urine drug", 1), ("drug testing", 1),
    ("substance abuse", 1), ("treatment monitoring", 1),
    ("molecular diagnostics", 1), ("molecular biology", 1), ("molecular", 1),
    ("genomics", 1), ("genetic testing", 1), ("genetics", 1),
    ("pharmacogenomics", 1), ("pgx", 1),
    ("oral fluid", 1), ("dna testing", 1), ("dna", 1), ("ngs", 1),
    ("next generation sequencing", 1), ("rapid testing", 1),
    ("point of care", 1), ("poc testing", 1), ("specialty lab", 1),
    # ── Tier B ──────────────────────────────────────────────────────────
    ("anatomic pathology", 2), ("surgical pathology", 2), ("cytology", 2),
    ("histology", 2), ("clinical chemistry", 2), ("hematology", 2),
    ("immunology", 2), ("serology", 2), ("clinical microbiology", 2),
    ("microbiology", 2), ("blood bank", 2), ("transfusion medicine", 2),
    ("endocrinology", 2), ("hormone testing", 2), ("allergy testing", 2),
    ("fertility", 2), ("reproductive", 2), ("women's health", 2), ("womens health", 2),
    ("urinalysis", 2), ("reference laboratory", 2), ("reference lab", 2),
    ("clinical pathology", 2),
    # ── Tier C ──────────────────────────────────────────────────────────
    ("clinical lab", 3), ("general lab", 3), ("routine testing", 3),
    ("hospital outreach", 3), ("outreach lab", 3), ("physician office", 3),
    ("diagnostics", 3), ("diagnostic", 3), ("clinical", 3),
]

_HIGH_VALUE_SIGNALS: list[str] = [
    "tox", "molecular", "genomic", "genetic", "pharmacogen", "pgx",
    "dna", "ngs", "spectrum", "precision", "specialized", "specialty",
    "analytical", "advanced", "elite", "premier", "prime", "core",
    "rapid", "point of care", "poc",
]

_LOW_VALUE_SIGNALS: list[str] = [
    "hospital", "health system", "university", "academic", "county",
    "public health", "veterans", " va ", "kaiser", "quest",
    "labcorp", "sonic", "aurora", "banner", "commonspirit",
]

# States with highest independent lab density and RCM billing complexity
_PRIORITY_STATES: frozenset = frozenset({
    "FL", "TX", "CA", "NY", "PA", "NJ", "GA", "IL", "OH", "NC",
    "AZ", "CO", "TN", "VA", "MD", "SC", "NV", "LA", "MO", "AL",
    "WA", "MA", "CT", "IN", "MI",
})


def score_lab_lead(org_name: str, lab_type: str = "", state: str = "") -> dict:
    """Score a lab lead 0-100 and assign Tier A / B / C for outreach prioritization.

    Higher score = better fit for MedPharma RCM / billing services.
    Tiers align with billing complexity:
      A = specialty (tox, molecular, genomics)  — highest value
      B = clinical complexity (pathology, hematology, immunology)
      C = general / commodity / hospital outreach

    Returns:
        {
          "tier": "A"|"B"|"C"|"Unknown",
          "score": int 0-100,
          "category": "Lab Lead",
          "lab_type_detected": str,
          "signals": list[str],
          "priority": "High"|"Medium"|"Low",
        }
    """
    org_l = (org_name or "").lower()
    type_l = re.sub(r"[^a-z0-9 ]+", " ", (lab_type or "").lower())
    state_u = (state or "").upper().strip()[:2]

    signals: list[str] = []
    score = 10           # base: it is a lab lead
    tier_num = 99        # 1=A  2=B  3=C  99=unknown

    # ── Lab-type tier — all matches, best (lowest) tier wins ──────────
    matched_type = ""
    for kw, t in _LAB_TIER_MAP:
        if kw in type_l or kw in org_l:
            if t < tier_num:
                tier_num = t
                matched_type = kw

    if tier_num == 1:
        score += 45
        signals.append(f"Tier A lab type: {matched_type}")
    elif tier_num == 2:
        score += 28
        signals.append(f"Tier B lab type: {matched_type}")
    elif tier_num == 3:
        score += 12
        signals.append(f"Tier C lab type: {matched_type}")

    # ── Org name quality signals ──────────────────────────────────────
    for sig in _HIGH_VALUE_SIGNALS:
        if sig in org_l:
            score += 12
            signals.append(f"Name signal (+): {sig}")
            break
    for sig in _LOW_VALUE_SIGNALS:
        if sig in org_l:
            score -= 25
            signals.append(f"Name signal (−): {sig} (large/public entity — low RCM fit)")
            break

    # ── State priority market ──────────────────────────────────────────
    if state_u in _PRIORITY_STATES:
        score += 15
        signals.append(f"Priority market: {state_u}")
    elif state_u:
        score += 5

    score = max(0, min(100, score))

    if tier_num == 1:
        tier, priority = "A", "High"
    elif tier_num == 2:
        tier, priority = "B", "Medium"
    elif tier_num == 3:
        tier, priority = "C", "Low"
    else:
        tier, priority = "Unknown", "Low"

    return {
        "tier": tier,
        "score": score,
        "category": "Lab Lead",
        "lab_type_detected": matched_type,
        "signals": signals,
        "priority": priority,
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
