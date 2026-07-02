"""Environment-based configuration + a factory that wires the hybrid engine.

Runs with ZERO credentials (absent creds => sandbox/mock mode). To go live, set:

    PVERIFY_CLIENT_ID, PVERIFY_CLIENT_SECRET   [, PVERIFY_BASE_URL]
    OFFICEALLY_USERNAME, OFFICEALLY_PASSWORD, OFFICEALLY_REALTIME_URL
    OFFICEALLY_SENDER_ID
    ELIG_SANDBOX=0    (only after real creds are in place)
"""
from __future__ import annotations

import os

from .hybrid import HybridEligibilityEngine, HybridStrategy
from .officeally import OfficeAllyProvider
from .pverify import PVerifyProvider


def _bool(v: str | None, default: bool) -> bool:
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def build_default_engine() -> HybridEligibilityEngine:
    force_sandbox = _bool(os.getenv("ELIG_SANDBOX"), default=True)
    pverify = PVerifyProvider(
        client_id=os.getenv("PVERIFY_CLIENT_ID", ""),
        client_secret=os.getenv("PVERIFY_CLIENT_SECRET", ""),
        base_url=os.getenv("PVERIFY_BASE_URL", "https://api.pverify.com"),
        sandbox=force_sandbox,
    )
    officeally = OfficeAllyProvider(
        username=os.getenv("OFFICEALLY_USERNAME", ""),
        password=os.getenv("OFFICEALLY_PASSWORD", ""),
        sender_id=os.getenv("OFFICEALLY_SENDER_ID", ""),
        realtime_url=os.getenv("OFFICEALLY_REALTIME_URL", ""),
        sandbox=force_sandbox,
    )
    return HybridEligibilityEngine(pverify, officeally, HybridStrategy.DISCOVER_THEN_VERIFY)
