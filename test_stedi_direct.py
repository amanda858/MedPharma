"""Real tests for the Stedi real-time eligibility connector (JSON 270/271).

These prove the connector is NOT a mock:
  • unconfigured  -> UNKNOWN, never ACTIVE (nothing fabricated)
  • bad MBI (CMS) -> UNKNOWN, request never sent
  • a real ACTIVE 271 (JSON) parses to ACTIVE with real benefit amounts
  • a real payer rejection (errors[]) is surfaced as an error, not hidden
  • an inactive response with a past plan-end date parses to TERMED
  • the request we build carries the payer id + normalized MBI + provider NPI
  • the raw X12 271 Stedi returns is kept as audit evidence

No network: `_post` is stubbed with canned, real-shaped Stedi JSON.

Run:  python3 test_stedi_direct.py
"""
from eligibility_hybrid.models import CoverageStatus, PatientRequest
from eligibility_hybrid.stedi import (StediProvider, build_stedi_request,
                                      parse_stedi_response)

VALID_MBI = "1EG4TE5MK73"


def _req(member_id=VALID_MBI):
    return PatientRequest(
        first_name="John", last_name="Doe", dob="1950-01-01",
        member_id=member_id, payer_name="Medicare",
        provider_npi="1234567893", provider_name="Acme Lab",
        date_of_service="2026-07-07", service_type_codes=["30"])


class _StubStedi(StediProvider):
    """Configured provider whose HTTP call is replaced by a canned response."""

    def __init__(self, canned, **kw):
        super().__init__(api_key="sk_test", provider_npi="1234567893",
                         provider_name="Acme Lab", **kw)
        self._canned = canned
        self.last_body = None

    def _post(self, body):
        self.last_body = body
        return self._canned


ACTIVE_JSON = {
    "id": "ec_test_active",
    "benefitsInformation": [
        {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"],
         "planCoverage": "MEDICARE PART B"},
        {"code": "C", "name": "Deductible", "coverageLevelCode": "IND",
         "serviceTypeCodes": ["30"], "benefitAmount": "257",
         "inPlanNetworkIndicatorCode": "Y"},
        {"code": "B", "name": "Co-Payment", "coverageLevelCode": "IND",
         "serviceTypeCodes": ["30"], "benefitAmount": "20",
         "inPlanNetworkIndicatorCode": "Y"},
        {"code": "A", "name": "Co-Insurance", "coverageLevelCode": "IND",
         "serviceTypeCodes": ["30"], "benefitPercent": "0.2"},
    ],
    "planDateInformation": {"eligibilityBegin": "20260101"},
    "subscriber": {"memberId": "1EG4TE5MK73", "firstName": "JOHN", "lastName": "DOE"},
    "payer": {"name": "MEDICARE"},
    "x12": "ISA*00*...~ST*271*0001*005010X279A1~EB*1*IND*30~SE*4*0001~IEA*1*1~",
}

REJECT_JSON = {
    "id": "ec_test_reject",
    "benefitsInformation": [],
    "errors": [{"code": "75", "description": "Subscriber/insured not found",
                "location": "subscriber"}],
    "x12": "ISA*00*...~AAA*Y**75*C~SE*3*0001~IEA*1*2~",
}

TERMED_JSON = {
    "id": "ec_test_termed",
    "benefitsInformation": [
        {"code": "6", "name": "Inactive", "serviceTypeCodes": ["30"]},
    ],
    "planDateInformation": {"planEnd": "20251231"},
    "subscriber": {"memberId": "1EG4TE5MK73"},
    "payer": {"name": "MEDICARE"},
}


def test_unconfigured_never_fabricates():
    prov = StediProvider()  # no key
    assert not prov.configured
    res = prov.verify(_req())
    assert res.status == CoverageStatus.UNKNOWN, res.status
    assert res.status != CoverageStatus.ACTIVE
    assert res.raw.get("configured") is False
    assert res.errors and "not configured" in res.errors[0].lower()
    print("PASS  unconfigured -> UNKNOWN, nothing fabricated")


def test_configured_requires_key_and_npi():
    assert not StediProvider(api_key="k").configured          # no NPI
    assert not StediProvider(provider_npi="123").configured   # no key
    assert StediProvider(api_key="k", provider_npi="123").configured
    print("PASS  configured only with API key + provider NPI")


def test_invalid_mbi_not_sent():
    prov = _StubStedi(ACTIVE_JSON)
    assert prov.configured
    res = prov.verify(_req(member_id="123456789AB"))  # not a valid MBI
    assert res.status == CoverageStatus.UNKNOWN
    assert res.raw.get("sent") is False
    assert prov.last_body is None, "invalid MBI must not reach the wire"
    print("PASS  invalid MBI -> request never sent to CMS")


def test_build_request_structure():
    body = build_stedi_request(_req(), "CMS", "1234567893", "Acme Lab")
    assert body["tradingPartnerServiceId"] == "CMS"
    assert body["subscriber"]["memberId"] == "1EG4TE5MK73"
    assert body["subscriber"]["dateOfBirth"] == "19500101"   # dashes stripped
    assert body["subscriber"]["firstName"] == "John"
    assert body["provider"]["npi"] == "1234567893"
    assert body["provider"]["organizationName"] == "Acme Lab"
    assert body["encounter"]["serviceTypeCodes"] == ["30"]
    assert body["encounter"]["dateOfService"] == "20260707"
    print("PASS  request carries payer id + normalized MBI + provider NPI")


def test_parse_active():
    res = parse_stedi_response(ACTIVE_JSON, _req(), "stedi")
    assert res.status == CoverageStatus.ACTIVE, res.status
    assert res.benefit.deductible_total == 257.0, res.benefit.deductible_total
    assert res.benefit.copay == 20.0, res.benefit.copay
    assert res.benefit.coinsurance_pct == 20.0, res.benefit.coinsurance_pct
    assert res.effective_date == "2026-01-01", res.effective_date
    assert res.member_id == "1EG4TE5MK73"
    assert res.plan_name == "MEDICARE PART B"
    assert not res.errors
    assert res.raw.get("x12_271", "").startswith("ISA*"), "raw 271 kept as evidence"
    print("PASS  ACTIVE JSON -> ACTIVE with real deductible/copay/coinsurance")


def test_parse_rejection_surfaced():
    res = parse_stedi_response(REJECT_JSON, _req(), "stedi")
    assert res.status == CoverageStatus.UNKNOWN
    assert res.errors, "payer rejection must be surfaced, not hidden"
    assert "not found" in res.errors[0].lower(), res.errors
    assert res.confidence == 0.0
    print("PASS  payer rejection surfaced as an error (not silent Unknown)")


def test_parse_termed():
    res = parse_stedi_response(TERMED_JSON, _req(), "stedi")
    assert res.status == CoverageStatus.TERMED, res.status
    assert res.term_date == "2025-12-31", res.term_date
    print("PASS  inactive response with past plan-end -> TERMED")


def test_end_to_end_active_keeps_evidence():
    prov = _StubStedi(ACTIVE_JSON)
    res = prov.verify(_req())
    assert res.status == CoverageStatus.ACTIVE
    assert prov.last_body["tradingPartnerServiceId"] == "CMS"
    assert res.raw.get("request_json"), "request JSON kept as evidence"
    assert res.raw.get("x12_271", "").startswith("ISA*")
    print("PASS  end-to-end verify() -> ACTIVE and keeps request + 271 evidence")


def test_coinsurance_percent_forms():
    decimal = parse_stedi_response(
        {"benefitsInformation": [{"code": "A", "benefitPercent": "0.2"}]},
        _req(), "stedi")
    whole = parse_stedi_response(
        {"benefitsInformation": [{"code": "A", "benefitPercent": "20"}]},
        _req(), "stedi")
    assert decimal.benefit.coinsurance_pct == 20.0, decimal.benefit.coinsurance_pct
    assert whole.benefit.coinsurance_pct == 20.0, whole.benefit.coinsurance_pct
    print("PASS  coinsurance parses from both 0.2 and 20 forms")


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
