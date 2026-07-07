"""Real tests for the DIRECT Medicare (HETS) 270/271 connector.

These prove the connector is NOT a mock:
  • unconfigured  -> UNKNOWN, never ACTIVE (nothing fabricated)
  • bad MBI       -> UNKNOWN, request never sent
  • a real ACTIVE 271 parses to ACTIVE with real benefit amounts
  • a real AAA rejection is surfaced as an error, not hidden as "Unknown"
  • an inactive 271 with a past plan-end date parses to TERMED
  • the 270 we build is a structurally valid 005010X279A1 with the MBI + right SE count

Run:  python3 test_hets_direct.py
"""
from eligibility_hybrid.hets import (HETSProvider, build_hets_270, is_valid_mbi,
                                     parse_hets_271)
from eligibility_hybrid.models import CoverageStatus, PatientRequest

VALID_MBI = "1EG4TE5MK73"   # canonical CMS sample MBI format


def _req(member_id=VALID_MBI):
    return PatientRequest(
        first_name="John", last_name="Doe", dob="1950-01-01",
        member_id=member_id, payer_name="Medicare",
        provider_npi="1234567893", provider_name="Acme Lab",
        date_of_service="2026-07-07", service_type_codes=["30"])


def _seg(*segs):
    return "~".join(segs) + "~"


ACTIVE_271 = _seg(
    "ISA*00*          *00*          *ZZ*CMS            *ZZ*SUBMIT12345   "
    "*260707*1200*^*00501*000000001*0*P*:",
    "GS*HB*CMS*SUBMIT12345*20260707*1200*1*X*005010X279A1",
    "ST*271*0001*005010X279A1",
    "BHT*0022*11*000000001*20260707*1200",
    "HL*1**20*1",
    "NM1*PR*2*MEDICARE*****PI*CMS",
    "HL*2*1*21*1",
    "NM1*1P*2*ACME LAB*****XX*1234567893",
    "HL*3*2*22*0",
    "TRN*2*000000001*SUBMIT12345",
    "NM1*IL*1*DOE*JOHN****MI*1EG4TE5MK73",
    "DMG*D8*19500101",
    "DTP*346*D8*20260101",
    "EB*1*IND*30**MEDICARE PART B",
    "EB*C*IND*30***29*257",       # deductible $257 (EB07)
    "EB*B*IND*30***27*20",        # copay $20     (EB07)
    "EB*A*IND*30*****0.20",       # coinsurance 20% (EB08)
    "SE*17*0001", "GE*1*1", "IEA*1*000000001")

REJECT_271 = _seg(
    "ISA*00*          *00*          *ZZ*CMS            *ZZ*SUBMIT12345   "
    "*260707*1200*^*00501*000000002*0*P*:",
    "GS*HB*CMS*SUBMIT12345*20260707*1200*1*X*005010X279A1",
    "ST*271*0001*005010X279A1",
    "BHT*0022*11*000000002*20260707*1200",
    "HL*1**20*1", "NM1*PR*2*MEDICARE*****PI*CMS",
    "HL*2*1*21*1", "NM1*1P*2*ACME LAB*****XX*1234567893",
    "HL*3*2*22*0",
    "NM1*IL*1*DOE*JOHN****MI*1EG4TE5MK73",
    "AAA*Y**67*C",                # 67 = Patient not found
    "SE*11*0001", "GE*1*1", "IEA*1*000000002")

TERMED_271 = _seg(
    "ISA*00*          *00*          *ZZ*CMS            *ZZ*SUBMIT12345   "
    "*260707*1200*^*00501*000000003*0*P*:",
    "GS*HB*CMS*SUBMIT12345*20260707*1200*1*X*005010X279A1",
    "ST*271*0001*005010X279A1", "BHT*0022*11*000000003*20260707*1200",
    "HL*1**20*1", "NM1*PR*2*MEDICARE*****PI*CMS",
    "HL*2*1*21*1", "NM1*1P*2*ACME LAB*****XX*1234567893",
    "HL*3*2*22*0", "NM1*IL*1*DOE*JOHN****MI*1EG4TE5MK73",
    "EB*6*IND*30**MEDICARE PART B",   # 6 = Inactive
    "DTP*347*D8*20251231",            # plan end in the past
    "SE*12*0001", "GE*1*1", "IEA*1*000000003")


def test_unconfigured_never_fabricates():
    prov = HETSProvider()  # no endpoint / creds
    assert not prov.configured
    res = prov.verify(_req())
    assert res.status == CoverageStatus.UNKNOWN, res.status
    assert res.status != CoverageStatus.ACTIVE
    assert res.raw.get("configured") is False
    assert res.errors and "not configured" in res.errors[0].lower()
    print("PASS  unconfigured -> UNKNOWN, nothing fabricated")


def test_invalid_mbi_not_sent():
    prov = HETSProvider(endpoint_url="https://hets.example/core",
                        submitter_id="SUBMIT12345", username="u", password="p")
    assert prov.configured
    res = prov.verify(_req(member_id="123456789AB"))  # not a valid MBI
    assert res.status == CoverageStatus.UNKNOWN
    assert res.raw.get("sent") is False
    print("PASS  invalid MBI -> request never sent")


def test_mbi_validator():
    assert is_valid_mbi("1EG4TE5MK73")
    assert is_valid_mbi("1eg4te5mk73")          # case-insensitive
    assert is_valid_mbi("1EG4-TE5-MK73")        # dashes tolerated
    assert not is_valid_mbi("1SG4TE5MK73")      # S not allowed at pos 2
    assert not is_valid_mbi("1EG4TE5MK7")       # 10 chars
    assert not is_valid_mbi("0EG4TE5MK73")      # pos 1 must be 1-9
    assert not is_valid_mbi("")
    print("PASS  MBI validator matches CMS positional spec")


def test_build_270_structure():
    x = build_hets_270(_req(), submitter_id="SUBMIT12345")
    assert "ST*270*0001*005010X279A1" in x
    assert "1EG4TE5MK73" in x                    # MBI present
    assert "NM1*IL*1*DOE*JOHN****MI*1EG4TE5MK73" in x
    assert x.startswith("ISA*")
    assert x.rstrip("~").endswith("IEA*1*" + x.split("*00501*")[1][:9])
    # SE count == segments ST..SE inclusive (13 for this inquiry)
    assert "SE*13*0001" in x, x
    print("PASS  270 is a structurally valid 005010X279A1 with the MBI")


def test_parse_active():
    res = parse_hets_271(ACTIVE_271, _req(), "hets")
    assert res.status == CoverageStatus.ACTIVE, res.status
    assert res.benefit.deductible_total == 257.0, res.benefit.deductible_total
    assert res.benefit.copay == 20.0, res.benefit.copay
    assert res.benefit.coinsurance_pct == 20.0, res.benefit.coinsurance_pct
    assert res.effective_date == "2026-01-01", res.effective_date
    assert res.member_id == "1EG4TE5MK73"
    assert not res.errors
    print("PASS  ACTIVE 271 -> ACTIVE with real deductible/copay/coinsurance")


def test_parse_rejection_surfaced():
    res = parse_hets_271(REJECT_271, _req(), "hets")
    assert res.status == CoverageStatus.UNKNOWN
    assert res.errors, "AAA rejection must be surfaced, not hidden"
    assert "not found" in res.errors[0].lower(), res.errors
    assert res.confidence == 0.0
    print("PASS  AAA rejection surfaced as an error (not silent Unknown)")


def test_parse_termed():
    res = parse_hets_271(TERMED_271, _req(), "hets")
    assert res.status == CoverageStatus.TERMED, res.status
    assert res.term_date == "2025-12-31", res.term_date
    print("PASS  inactive 271 with past plan-end -> TERMED")


def test_mime_wrapped_response_unwraps():
    wrapped = (
        "--boundary\r\nContent-Disposition: form-data; name=\"PayloadType\"\r\n\r\n"
        "X12_271_Response_005010X279A1\r\n--boundary\r\n"
        "Content-Disposition: form-data; name=\"Payload\"\r\n\r\n"
        + ACTIVE_271 + "\r\n--boundary--\r\n")
    res = parse_hets_271(wrapped, _req(), "hets")
    assert res.status == CoverageStatus.ACTIVE, res.status
    print("PASS  MIME-wrapped 271 payload is unwrapped and parsed")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    raise SystemExit(1 if failed else 0)
