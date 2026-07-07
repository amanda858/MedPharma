"""Runnable demo (sandbox mode):  python3 -m eligibility_hybrid.demo

Walks the hybrid engine through the three scenarios that matter:
  1. Known payer          -> pVerify real-time verify with rich benefits.
  2. Unknown / self-pay    -> pVerify defers, the discovery provider finds
                              coverage, then pVerify re-verifies it (the "merge").
  3. Termed coverage       -> flagged before the sample is ever run.
Plus per-CPT covered / prior-auth / patient-responsibility for each lab test.
"""
from __future__ import annotations

from .config import build_default_engine
from .models import CoverageResult, PatientRequest

SAMPLES = [
    PatientRequest(first_name="Marcus", last_name="Bell", dob="1974-03-12", gender="M",
                   payer_name="UnitedHealthcare", member_id="912345678",
                   cpt_codes=["87631", "87635"],
                   provider_npi="1972000000", provider_name="MedPharma Lab"),
    PatientRequest(first_name="Deja", last_name="Franklin", dob="1991-06-02", gender="F",
                   ssn_last4="4821", zip_code="33101",
                   cpt_codes=["87507", "87798"],
                   provider_npi="1972000000", provider_name="MedPharma Lab"),
    PatientRequest(first_name="Kevin", last_name="ONeil", dob="1988-11-19", gender="M",
                   payer_name="Cigna", member_id="U8842019",
                   cpt_codes=["87631"],
                   provider_npi="1972000000", provider_name="MedPharma Lab"),
]


def _print(res: CoverageResult, req: PatientRequest) -> None:
    print("=" * 78)
    tag = f"  ({req.payer_name})" if req.payer_known else "  [no insurance on file]"
    print(f"PATIENT   {req.full_name}  DOB {req.dob}{tag}")
    disc = " (DISCOVERED)" if res.discovered else ""
    print(f"COVERAGE  {res.status.value}{disc} via {res.source}  "
          f"-> {res.payer_name} [{res.payer_id}]  member {res.member_id}")
    b = res.benefit
    if b.copay is not None:
        print(f"BENEFIT   copay ${b.copay:.0f} | deductible "
              f"${b.deductible_met or 0:.0f}/${b.deductible_total or 0:.0f} | "
              f"coins {b.coinsurance_pct or 0:.0f}% | OOP "
              f"${b.oop_met or 0:.0f}/${b.oop_total or 0:.0f}")
    for c in res.per_cpt:
        pr = (f"patient ${c.patient_responsibility:.2f}"
              if c.patient_responsibility is not None else "")
        print(f"  CPT {c.cpt}  {c.status.value:<20}{pr:>16}   {c.description}")
    print("TRACE     " + "  ->  ".join(res.trace))


def main() -> None:
    engine = build_default_engine()
    mode = "SANDBOX (mock data, no credentials)" if engine.pverify.sandbox else "LIVE"
    print(f"\nMedPharma Hybrid Eligibility Engine  —  {mode}")
    print("pVerify (real-time benefits) + Stedi (free, self-serve 270/271),"
          " merged behind one resolve() call\n")
    for req in SAMPLES:
        _print(engine.resolve(req), req)
    print("=" * 78)


if __name__ == "__main__":
    main()
