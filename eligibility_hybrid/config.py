"""Environment-based configuration + a factory that wires the hybrid engine.

Runs with ZERO credentials (absent creds => sandbox/mock mode). The real-time
Verify path is free / self-serve — set one of:

    STEDI_API_KEY, STEDI_PROVIDER_NPI          (self-serve, live in minutes)
    HETS_ENDPOINT_URL + HETS_SUBMITTER_ID …    (direct CMS Medicare, free)

The optional pre-analytical gate tool can also use pVerify for rich benefits:

    PVERIFY_CLIENT_ID, PVERIFY_CLIENT_SECRET   [, PVERIFY_BASE_URL]
    ELIG_SANDBOX=0    (only after real creds are in place)
"""
from __future__ import annotations

import os

from .hets import HETSProvider
from .hybrid import HybridEligibilityEngine, HybridStrategy
from .pverify import PVerifyProvider
from .stedi import StediProvider


def _bool(v: str | None, default: bool) -> bool:
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def build_hets_provider() -> HETSProvider:
    """DIRECT CMS Medicare (FFS) eligibility — no clearinghouse.

    Configure via env once the CMS HETS submitter enrollment is complete:
        HETS_ENDPOINT_URL   CMS CORE connectivity endpoint
        HETS_SUBMITTER_ID   your assigned trading-partner / submitter ID
        HETS_USERNAME       CORE connectivity credentials …
        HETS_PASSWORD       … (or use a client cert instead)
        HETS_CLIENT_CERT / HETS_CLIENT_KEY   mutual-TLS cert (alternative auth)
        HETS_RECEIVER_ID    default 'CMS' — confirm in the HETS Companion Guide
        HETS_PAYER_ID       NM1*PR payer id — confirm in the HETS Companion Guide

    With none of these set the provider is simply "not configured" and refuses
    honestly; it never fabricates a result.
    """
    return HETSProvider(
        endpoint_url=os.getenv("HETS_ENDPOINT_URL", ""),
        submitter_id=os.getenv("HETS_SUBMITTER_ID", ""),
        username=os.getenv("HETS_USERNAME", ""),
        password=os.getenv("HETS_PASSWORD", ""),
        receiver_id=os.getenv("HETS_RECEIVER_ID", "CMS"),
        payer_id=os.getenv("HETS_PAYER_ID", ""),
        client_cert=os.getenv("HETS_CLIENT_CERT", ""),
        client_key=os.getenv("HETS_CLIENT_KEY", ""),
    )


def build_stedi_provider() -> StediProvider:
    """Real-time eligibility via Stedi — the fast, self-serve alternative to a
    multi-week CMS HETS enrollment.

    Configure via env (generate a key at portal.stedi.com in minutes):
        STEDI_API_KEY        your Stedi API key
        STEDI_PROVIDER_NPI   the requesting provider's NPI (required by Stedi)
        STEDI_PROVIDER_NAME  the provider/organization name
        STEDI_PAYER_ID       payer id (default 'CMS' = Original Medicare)
        STEDI_ENDPOINT_URL   override the default real-time endpoint (optional)
        STEDI_FORWARDED_FOR  origin IP chain for CMS traceability (optional)

    With no key set the provider is simply "not configured" and refuses
    honestly; it never fabricates a result.
    """
    return StediProvider(
        api_key=os.getenv("STEDI_API_KEY", ""),
        endpoint_url=os.getenv("STEDI_ENDPOINT_URL", ""),
        payer_id=os.getenv("STEDI_PAYER_ID", "CMS"),
        provider_npi=os.getenv("STEDI_PROVIDER_NPI", ""),
        provider_name=os.getenv("STEDI_PROVIDER_NAME", ""),
        forwarded_for=os.getenv("STEDI_FORWARDED_FOR", ""),
    )


def build_eligibility_provider():
    """Return the best-configured REAL eligibility provider.

    Preference: Stedi (self-serve API key, fastest to go live) → direct CMS
    HETS. If neither is configured, returns the (unconfigured) Stedi provider so
    the caller can surface the fastest path to go live. NEVER returns a
    fabricating/sandbox provider.
    """
    stedi = build_stedi_provider()
    if stedi.configured:
        return stedi
    hets = build_hets_provider()
    if hets.configured:
        return hets
    return stedi


def build_default_engine() -> HybridEligibilityEngine:
    """Wire the pre-analytical gate engine.

    Primary = pVerify (optional; sandbox/mock until real creds). Secondary =
    the free, self-serve Stedi real-time provider, so the gate's fallback
    verify is a genuinely free real 270/271 pipe — no paid clearinghouse and no
    Office Ally account required.
    """
    force_sandbox = _bool(os.getenv("ELIG_SANDBOX"), default=True)
    pverify = PVerifyProvider(
        client_id=os.getenv("PVERIFY_CLIENT_ID", ""),
        client_secret=os.getenv("PVERIFY_CLIENT_SECRET", ""),
        base_url=os.getenv("PVERIFY_BASE_URL", "https://api.pverify.com"),
        sandbox=force_sandbox,
    )
    return HybridEligibilityEngine(pverify, build_stedi_provider(),
                                   HybridStrategy.DISCOVER_THEN_VERIFY)
