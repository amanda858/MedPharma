"""MedPharma hybrid eligibility engine (pVerify + Office Ally).

Standalone foundation — deliberately NOT wired into the production hub yet.
Runs in sandbox (mock) mode with zero credentials; drop in real creds via env
(see config.py) to go live.

    from eligibility_hybrid import build_default_engine, PatientRequest
    engine = build_default_engine()
    result = engine.resolve(PatientRequest(first_name="Jane", last_name="Doe",
                                           dob="1980-01-01", payer_name="Aetna",
                                           member_id="W123", cpt_codes=["87631"]))
"""
from .config import build_default_engine
from .gate import AccessionGate, AccessionResult, CptDisposition, Disposition
from .hybrid import HybridEligibilityEngine, HybridStrategy
from .models import (Benefit, CoverageResult, CoverageStatus, CptCoverage,
                     CptStatus, EligibilityProvider, PatientRequest, ProviderError)
from .officeally import OfficeAllyProvider
from .policy import (MedNecResult, check_medical_necessity, is_prior_auth_required,
                     is_traditional_medicare)
from .prior_auth import (PaChannel, PaStatus, PriorAuthEngine, PriorAuthRequest,
                         PriorAuthResult)
from .pverify import PVerifyProvider

__all__ = [
    "PatientRequest", "CoverageResult", "Benefit", "CptCoverage",
    "CoverageStatus", "CptStatus", "EligibilityProvider", "ProviderError",
    "PVerifyProvider", "OfficeAllyProvider",
    "HybridEligibilityEngine", "HybridStrategy", "build_default_engine",
    "AccessionGate", "AccessionResult", "CptDisposition", "Disposition",
    "MedNecResult", "check_medical_necessity", "is_prior_auth_required",
    "is_traditional_medicare",
    "PriorAuthEngine", "PriorAuthRequest", "PriorAuthResult", "PaStatus", "PaChannel",
]
