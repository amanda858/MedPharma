#!/usr/bin/env python3
"""Stedi connection self-test — prove the eligibility pipe with one command.

WHAT THIS IS FOR
    The eligibility code is built and deployed. The only thing it needs to go
    live is a Stedi API key (an account under *your* identity — the one step
    that can't be done in code). This script turns that key into instant proof:
    it drives our REAL Stedi provider against Stedi's REAL endpoint and prints
    exactly what came back.

GET A FREE TEST KEY (~2 minutes, no BAA, no card, no NPI enrollment)
    1. Sign up at https://portal.stedi.com/  (email only — this is a free
       "sandbox" account, test mode only).
    2. Account menu -> API Keys -> Generate new API Key -> Mode: **Test**.
    3. Copy the key and export it, then run this script:

        export STEDI_API_KEY='your-test-key'
        python3 scripts/stedi_selftest.py

    A **test** key never transmits PHI and is never billed. It returns realistic
    MOCK benefits for real payers so you can see the shape of production data.
    When you later want to check real patients, generate a **Production** key
    (that path needs a Stedi production account + BAA) and set the same env var.

HONESTY
    This sends whatever payer/patient you configure and prints the payer's real
    response verbatim — active coverage, or an AAA rejection, or a transport
    error. Nothing here fabricates an "Active" result.

Optional overrides (env or CLI flags):
    STEDI_PAYER_ID / --payer        default: Aetna (a documented mock payer)
    STEDI_PROVIDER_NPI / --npi      default: 1999999984 (valid check-digit dummy)
    STEDI_PROVIDER_NAME             default: "ACME Health Services"
    --member / --first / --last / --dob   subscriber fields (default: doc example)

Exit codes: 0 = reached Stedi (connectivity proven, even on an AAA rejection),
    1 = transport/auth failure, 2 = no API key configured.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Make `eligibility_hybrid` importable when run from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from eligibility_hybrid.models import PatientRequest, ProviderError  # noqa: E402
from eligibility_hybrid.stedi import (StediProvider, build_stedi_request,  # noqa: E402
                                      parse_stedi_response)


def _fmt_money(v):
    return "—" if v is None else f"${v:,.2f}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Stedi eligibility connection self-test")
    ap.add_argument("--payer", default=os.getenv("STEDI_PAYER_ID") or "AETNA",
                    help="payer id / alias (default: AETNA, a documented mock payer)")
    ap.add_argument("--npi", default=os.getenv("STEDI_PROVIDER_NPI") or "1999999984",
                    help="requesting provider NPI (default: 1999999984 dummy)")
    ap.add_argument("--member", default=os.getenv("STEDI_TEST_MEMBER") or "1234567890")
    ap.add_argument("--first", default=os.getenv("STEDI_TEST_FIRST") or "Jane")
    ap.add_argument("--last", default=os.getenv("STEDI_TEST_LAST") or "Doe")
    ap.add_argument("--dob", default=os.getenv("STEDI_TEST_DOB") or "1900-01-01")
    args = ap.parse_args()

    api_key = os.getenv("STEDI_API_KEY", "").strip()
    if not api_key:
        print(__doc__)
        print("\n✗ STEDI_API_KEY is not set — nothing to test yet.\n"
              "  Follow the steps above to mint a free TEST key, then re-run.")
        return 2

    provider = StediProvider(
        api_key=api_key,
        endpoint_url=os.getenv("STEDI_ENDPOINT_URL", ""),
        payer_id=args.payer,
        provider_npi=args.npi,
        provider_name=os.getenv("STEDI_PROVIDER_NAME", "") or "ACME Health Services",
        forwarded_for=os.getenv("STEDI_FORWARDED_FOR", ""),
    )

    req = PatientRequest(
        first_name=args.first, last_name=args.last, dob=args.dob,
        member_id=args.member, payer_id=args.payer, service_type_codes=["30"],
        provider_npi=args.npi, provider_name=provider.provider_name,
    )

    key_kind = "TEST (mock data, no PHI, no charge)" if api_key.lower().startswith("test") \
        else "as-configured (may be a PRODUCTION key — real payer traffic)"
    print("=" * 68)
    print("Stedi eligibility connection self-test")
    print("=" * 68)
    print(f"  endpoint : {provider.endpoint_url}")
    print(f"  key kind : {key_kind}")
    print(f"  payer    : {args.payer}")
    print(f"  provider : {provider.provider_name} (NPI {args.npi})")
    print(f"  patient  : {args.first} {args.last}  DOB {args.dob}  member {args.member}")
    print("-" * 68)

    # Send directly (bypass the verify() MBI guard) so this is a pure
    # connectivity + auth probe — we report exactly what Stedi returns.
    body = build_stedi_request(req, args.payer, args.npi, provider.provider_name)
    try:
        data = provider._post(body)
    except ProviderError as e:
        msg = str(e.message if hasattr(e, "message") else e)
        print(f"✗ Could not reach/authenticate with Stedi:\n    {msg}")
        if "401" in msg or "403" in msg:
            print("  → That looks like an auth error. Double-check STEDI_API_KEY "
                  "(no 'Bearer ' prefix; use the raw key).")
        return 1

    result = parse_stedi_response(data, req, "stedi")
    b = result.benefit

    print(f"✓ Reached Stedi. eligibility check id: {data.get('id', '—')}")
    if data.get("status"):
        print(f"  transport status: {data.get('status')}")
    print(f"  coverage status : {result.status.value}")
    if result.payer_name:
        print(f"  payer           : {result.payer_name}")
    if b and (b.copay is not None or b.deductible_total is not None
              or b.coinsurance_pct is not None or b.oop_total is not None):
        print(f"  copay {_fmt_money(b.copay)} · deductible {_fmt_money(b.deductible_total)}"
              f" · coinsurance {'—' if b.coinsurance_pct is None else str(b.coinsurance_pct) + '%'}"
              f" · OOP max {_fmt_money(b.oop_total)}")
    if result.effective_date or result.term_date:
        print(f"  effective {result.effective_date or '—'} · term {result.term_date or '—'}")
    if result.errors:
        print("  payer errors (surfaced honestly, not hidden):")
        for err in result.errors:
            print(f"    • {err}")
    raw_271 = result.raw.get("x12_271") or ""
    print(f"  raw X12 271 evidence: {'yes (' + str(len(raw_271)) + ' bytes)' if raw_271 else 'none'}")
    print("-" * 68)

    if result.errors and result.status.value == "Unknown":
        print("Connectivity + auth WORK. The payer returned a rejection for these\n"
              "test values (Stedi mock requests require the payer's EXACT fixture\n"
              "values). For a guaranteed active-coverage demo, open the Stedi portal\n"
              "→ New eligibility check with Test mode ON and pick a prefilled mock,\n"
              "or pass the documented fixture via --member/--first/--last/--dob.")
    else:
        print("Connectivity + auth WORK and benefits parsed. The eligibility pipe is\n"
              "proven end-to-end. Set the same STEDI_API_KEY on the server (Render)\n"
              "and the hub's Verify button runs real checks.")
    print("\nFull raw Stedi JSON:")
    print(json.dumps(data, indent=2)[:4000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
