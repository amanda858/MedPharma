"""Runnable demo (sandbox):  python3 -m eligibility_hybrid.demo_gate

Shows the full pre-analytical gate — eligibility + medical necessity + prior
auth -> one disposition per test, with the dollars — across the scenarios a lab
hits every day.
"""
from __future__ import annotations

from .config import build_default_engine
from .gate import AccessionGate, Disposition
from .models import PatientRequest
from .prior_auth import PriorAuthEngine

SAMPLES = [
    ("Respiratory panel, good Dx -> PA auto-approved",
     PatientRequest(first_name="Marcus", last_name="Bell", dob="1974-03-12", gender="M",
                    payer_name="UnitedHealthcare", member_id="912345678",
                    cpt_codes=["87631", "87635"], icd10_codes=["J12.81", "R05.9"],
                    provider_npi="1972000000", provider_name="MedPharma Lab")),
    ("Self-pay -> Office Ally discovers Medicaid, then gate",
     PatientRequest(first_name="Deja", last_name="Franklin", dob="1991-06-02", gender="F",
                    ssn_last4="4821", zip_code="33101",
                    cpt_codes=["87507", "87798"], icd10_codes=["A08.4", "N39.0"],
                    provider_npi="1972000000", provider_name="MedPharma Lab")),
    ("Termed coverage -> self-pay",
     PatientRequest(first_name="Kevin", last_name="ONeil", dob="1988-11-19", gender="M",
                    payer_name="Cigna", member_id="U8842019",
                    cpt_codes=["87631"], icd10_codes=["J20.9"],
                    provider_npi="1972000000")),
    ("Unsupported Dx -> medical-necessity hold",
     PatientRequest(first_name="Nadia", last_name="Cole", dob="1969-09-09", gender="F",
                    payer_name="Aetna", member_id="W5567781",
                    cpt_codes=["87631"], icd10_codes=["Z00.00"],
                    provider_npi="1972000000")),
    ("Medicare, non-covered Dx -> ABN",
     PatientRequest(first_name="Harold", last_name="Metz", dob="1948-02-14", gender="M",
                    payer_name="Medicare Part B", member_id="1EG4TE5MK72",
                    cpt_codes=["87631"], icd10_codes=["Z00.00"],
                    provider_npi="1972000000")),
]

_ICON = {
    Disposition.CLEAR_TO_RUN: "[GO ]",
    Disposition.HOLD_PRIOR_AUTH: "[PA ]",
    Disposition.HOLD_MED_NECESSITY: "[MN ]",
    Disposition.GET_ABN: "[ABN]",
    Disposition.SELF_PAY: "[SP ]",
    Disposition.DENY_RISK: "[ X ]",
}


def _print(title, res):
    print("=" * 84)
    print(title)
    disc = "  (DISCOVERED)" if res.coverage.discovered else ""
    print(f"PATIENT   {res.patient}   coverage: {res.coverage.status.value} "
          f"via {res.coverage.source} -> {res.coverage.payer_name}{disc}")
    print(f"ACCESSION {_ICON.get(res.overall, '')} {res.overall.value}"
          f"    expected value ${res.total_expected_value:,.2f}")
    for ln in res.lines:
        pr = f"pt ${ln.patient_responsibility:.0f}" if ln.patient_responsibility is not None else ""
        auth = f"  auth {ln.auth_number}" if ln.auth_number else ""
        print(f"  {_ICON.get(ln.disposition, '')} CPT {ln.cpt}  {ln.disposition.value:<22} "
              f"EV ${ln.expected_value or 0:>8.2f}  {pr}{auth}")
        for r in ln.reasons:
            print(f"          - {r}")
        for a in ln.actions:
            print(f"          -> {a}")


def main():
    engine = build_default_engine()
    gate = AccessionGate(engine, PriorAuthEngine(sandbox=engine.pverify.sandbox),
                         auto_submit_pa=True)
    mode = "SANDBOX (mock data, no credentials)" if engine.pverify.sandbox else "LIVE"
    print(f"\nMedPharma Pre-Analytical Gate — {mode}")
    print("eligibility (pVerify + Office Ally) + medical necessity (LCD/NCD) + prior auth"
          " -> one disposition per test\n")
    for title, req in SAMPLES:
        _print(title, gate.evaluate(req))
    print("=" * 84)


if __name__ == "__main__":
    main()
