"""Normalized, vendor-agnostic data models for the hybrid eligibility engine.

Every provider response (Stedi, CMS HETS, pVerify, …) is mapped into
`CoverageResult`, so the rest of the application only ever deals with ONE schema
regardless of which vendor (or combination) answered.
"""
from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import date
from enum import Enum
from typing import Optional


def stable_hash(*parts: str) -> int:
    """Deterministic, process-stable non-negative int hash (for mock data)."""
    joined = "|".join(p or "" for p in parts)
    return int(hashlib.sha256(joined.encode()).hexdigest()[:12], 16)


class CoverageStatus(str, Enum):
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    TERMED = "Termed"
    PENDING = "Pending"
    UNKNOWN = "Unknown"


class CptStatus(str, Enum):
    COVERED = "Covered"
    NOT_COVERED = "Not Covered"
    PRIOR_AUTH = "Prior Auth Required"
    UNKNOWN = "Unknown"


@dataclass
class PatientRequest:
    """Everything a verifier types on the single intake screen."""
    first_name: str
    last_name: str
    dob: str                       # YYYY-MM-DD
    gender: str = "U"              # M / F / U
    member_id: Optional[str] = None
    payer_name: Optional[str] = None
    payer_id: Optional[str] = None
    ssn_last4: Optional[str] = None
    zip_code: Optional[str] = None
    state: Optional[str] = None
    date_of_service: Optional[str] = None      # YYYY-MM-DD; defaults to today
    cpt_codes: list[str] = field(default_factory=list)
    icd10_codes: list[str] = field(default_factory=list)   # ordering Dx
    service_type_codes: list[str] = field(default_factory=lambda: ["30"])
    provider_npi: str = ""
    provider_name: str = ""

    @property
    def payer_known(self) -> bool:
        return bool(self.payer_id or self.payer_name)

    @property
    def dos(self) -> str:
        return self.date_of_service or date.today().isoformat()

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


@dataclass
class Benefit:
    copay: Optional[float] = None
    deductible_total: Optional[float] = None
    deductible_met: Optional[float] = None
    coinsurance_pct: Optional[float] = None
    oop_total: Optional[float] = None
    oop_met: Optional[float] = None

    @property
    def deductible_remaining(self) -> Optional[float]:
        if self.deductible_total is None:
            return None
        return max(0.0, self.deductible_total - (self.deductible_met or 0.0))

    @property
    def oop_remaining(self) -> Optional[float]:
        if self.oop_total is None:
            return None
        return max(0.0, self.oop_total - (self.oop_met or 0.0))


@dataclass
class CptCoverage:
    cpt: str
    status: CptStatus = CptStatus.UNKNOWN
    description: str = ""
    allowed_amount: Optional[float] = None
    patient_responsibility: Optional[float] = None
    plan_pays: Optional[float] = None
    note: str = ""


@dataclass
class CoverageResult:
    status: CoverageStatus
    source: str                    # "stedi" | "hets" | "pverify" | "hybrid"
    payer_name: str = ""
    payer_id: str = ""
    plan_name: str = ""
    member_id: str = ""
    group_number: str = ""
    subscriber_name: str = ""
    effective_date: str = ""
    term_date: str = ""
    benefit: Benefit = field(default_factory=Benefit)
    prior_auth_required: Optional[bool] = None
    per_cpt: list[CptCoverage] = field(default_factory=list)
    discovered: bool = False       # coverage was found from demographics alone
    confidence: float = 1.0
    trace: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.status == CoverageStatus.ACTIVE

    def to_dict(self) -> dict:
        return json.loads(json.dumps(self, default=_json_default))


def _json_default(o):
    if isinstance(o, Enum):
        return o.value
    if hasattr(o, "__dataclass_fields__"):
        return asdict(o)
    return str(o)


class ProviderError(Exception):
    """Raised by an adapter when a call fails (network, auth, unsupported payer)."""

    def __init__(self, provider: str, message: str, retryable: bool = False):
        super().__init__(f"[{provider}] {message}")
        self.provider = provider
        self.retryable = retryable


class EligibilityProvider(ABC):
    """Common interface every vendor adapter implements."""
    name: str = "base"

    @abstractmethod
    def verify(self, req: PatientRequest) -> CoverageResult:
        """Real-time eligibility for a KNOWN payer/member."""

    def discover(self, req: PatientRequest) -> Optional[CoverageResult]:
        """Find coverage from demographics when the payer is unknown."""
        return None

    def supports_discovery(self) -> bool:
        return False
