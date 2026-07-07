"""MedPharma Coverage Intercept — a proprietary MedPharma product.

Ownership & provenance
----------------------
This package is MedPharma intellectual property, independently invented. Its
decision logic — coverage resolution, medical necessity, prior-auth decisioning,
the accession disposition gate, and the claim-integrity intercept — is ORIGINAL
work built on PUBLIC standards (X12 270/271/278, HL7 FHIR Da Vinci, CMS
LCD/NCD, NCCI/MUE, CLIA/QW, Palmetto GBA MolDX). Those are open standards and
government rules; MedPharma's implementation of them is its own.

Third-party clearinghouses are reached only through pluggable CONNECTORS behind
MedPharma's own `EligibilityProvider` interface. The connectors talk to each
vendor's PUBLISHED API and contain no vendor proprietary code — the vendor is a
swappable data source ("buy the pipes"); the intelligence is MedPharma's
("build the brain"). Sandbox mocks are MedPharma-authored, not vendor artifacts.

Runs in sandbox (mock) mode with zero credentials; drop in real creds via env
(see config.py) to go live.

    from eligibility_hybrid import build_default_engine, PatientRequest
    engine = build_default_engine()
    result = engine.resolve(PatientRequest(first_name="Jane", last_name="Doe",
                                           dob="1980-01-01", payer_name="Aetna",
                                           member_id="W123", cpt_codes=["87631"]))
"""
from .config import build_default_engine, build_hets_provider
from .config import build_stedi_provider, build_eligibility_provider
from .gate import AccessionGate, AccessionResult, CptDisposition, Disposition
from .hets import HETSProvider, build_hets_270, is_valid_mbi, parse_hets_271
from .hybrid import HybridEligibilityEngine, HybridStrategy
from .intercept import (ENGINE_NAME, METHOD, PRODUCT_NAME, Finding, run_intercept,
                        summarize_findings)
from .models import (Benefit, CoverageResult, CoverageStatus, CptCoverage,
                     CptStatus, EligibilityProvider, PatientRequest, ProviderError)
from .policy import (MedNecResult, check_medical_necessity, is_prior_auth_required,
                     is_traditional_medicare)
from .prior_auth import (PaChannel, PaStatus, PriorAuthEngine, PriorAuthRequest,
                         PriorAuthResult)
from .pverify import PVerifyProvider
from .stedi import StediProvider, build_stedi_request, parse_stedi_response
from .stedi_payers import StediPayers, build_stedi_payers, resolve_payer_id

PRODUCT = PRODUCT_NAME

__all__ = [
    "PatientRequest", "CoverageResult", "Benefit", "CptCoverage",
    "CoverageStatus", "CptStatus", "EligibilityProvider", "ProviderError",
    "PVerifyProvider",
    "HybridEligibilityEngine", "HybridStrategy", "build_default_engine",
    "HETSProvider", "build_hets_provider", "build_hets_270", "parse_hets_271",
    "is_valid_mbi",
    "StediProvider", "build_stedi_provider", "build_stedi_request",
    "parse_stedi_response", "build_eligibility_provider",
    "StediPayers", "build_stedi_payers", "resolve_payer_id",
    "AccessionGate", "AccessionResult", "CptDisposition", "Disposition",
    "MedNecResult", "check_medical_necessity", "is_prior_auth_required",
    "is_traditional_medicare",
    "PriorAuthEngine", "PriorAuthRequest", "PriorAuthResult", "PaStatus", "PaChannel",
    "run_intercept", "summarize_findings", "Finding",
    "PRODUCT_NAME", "PRODUCT", "ENGINE_NAME", "METHOD",
]
