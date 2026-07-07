"""Offline tests for the Stedi Payers crosswalk + per-patient payer routing.

No network: the Payers client's HTTP GET and the eligibility connector's POST
are stubbed, so these assert parsing + routing logic only. Run:

    python3 test_stedi_payers.py
"""
import unittest

from eligibility_hybrid import (StediPayers, StediProvider, PatientRequest,
                                CoverageStatus, build_stedi_payers)


class FakePayers:
    """Stands in for StediPayers.resolve_payer_id, recording lookups."""

    def __init__(self, mapping):
        self.mapping = {k.lower(): v for k, v in mapping.items()}
        self.calls = []

    def resolve_payer_id(self, query):
        self.calls.append(query)
        return self.mapping.get((query or "").lower())


class TestStediPayers(unittest.TestCase):
    def test_unconfigured_is_inert(self):
        p = StediPayers(api_key="")
        self.assertFalse(p.configured)
        self.assertEqual(p.search("aetna"), [])          # no network attempted
        self.assertIsNone(p.resolve_payer_id("aetna"))
        self.assertEqual(p.list_payers(), ([], ""))

    def test_search_parses_records(self):
        p = StediPayers(api_key="k")
        p._get = lambda path, params=None: {
            "items": [{"payer": {
                "stediId": "AETNA", "primaryPayerId": "60054",
                "displayName": "Aetna", "aliases": ["AET"],
                "programs": ["COMMERCIAL"],
                "transactionSupport": {"eligibilityCheck": "SUPPORTED"}}}]}
        hits = p.search("aetna")
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["primary_payer_id"], "60054")
        self.assertEqual(hits[0]["display_name"], "Aetna")
        self.assertTrue(hits[0]["eligibility_supported"])
        self.assertFalse(hits[0]["enrollment_required"])

    def test_resolve_returns_primary_payer_id(self):
        p = StediPayers(api_key="k")
        p._get = lambda path, params=None: {
            "items": [{"payer": {"stediId": "HUMAN", "primaryPayerId": "61101",
                                 "displayName": "Humana",
                                 "transactionSupport": {"eligibilityCheck": "SUPPORTED"}}}]}
        self.assertEqual(p.resolve_payer_id("Humana Medicare"), "61101")

    def test_enrollment_required_flag(self):
        p = StediPayers(api_key="k")
        p._get = lambda path, params=None: {
            "items": [{"payer": {"stediId": "GUFCO", "primaryPayerId": "54771",
                                 "displayName": "Highmark",
                                 "transactionSupport": {"eligibilityCheck": "ENROLLMENT_REQUIRED"}}}]}
        hit = p.search("highmark")[0]
        self.assertTrue(hit["eligibility_supported"])
        self.assertTrue(hit["enrollment_required"])

    def test_list_payers_pagination_token(self):
        p = StediPayers(api_key="k")
        p._get = lambda path, params=None: {
            "items": [{"primaryPayerId": "1"}], "nextPageToken": "tok"}
        rows, token = p.list_payers()
        self.assertEqual(len(rows), 1)
        self.assertEqual(token, "tok")

    def test_build_from_env_default_host(self):
        p = build_stedi_payers(api_key="k")
        self.assertTrue(p.base_url.startswith("https://payers.us.stedi.com/"))


class TestStediRouting(unittest.TestCase):
    def _provider(self, fake, **kw):
        prov = StediProvider(api_key="k", provider_npi="1999999984",
                             provider_name="ACME Lab", payers_client=fake, **kw)
        return prov

    def _stub_post(self, prov, captured):
        prov._post = lambda body: (captured.update(body)
                                   or {"benefitsInformation": [{"code": "1"}],
                                       "x12": "ISA*..."})

    def test_explicit_payer_id_wins(self):
        fake = FakePayers({"aetna": "99999"})
        prov = self._provider(fake)
        captured = {}
        self._stub_post(prov, captured)
        req = PatientRequest(first_name="Jane", last_name="Doe", dob="1980-01-01",
                             member_id="W123", payer_id="60054", payer_name="Aetna")
        res = prov.verify(req)
        self.assertEqual(captured["tradingPartnerServiceId"], "60054")
        self.assertEqual(res.status, CoverageStatus.ACTIVE)
        self.assertEqual(fake.calls, [])  # explicit id -> no resolve call

    def test_typed_payer_name_resolves(self):
        fake = FakePayers({"humana medicare": "61101"})
        prov = self._provider(fake)
        captured = {}
        self._stub_post(prov, captured)
        req = PatientRequest(first_name="Jane", last_name="Doe", dob="1980-01-01",
                             member_id="H999", payer_name="Humana Medicare")
        res = prov.verify(req)
        self.assertEqual(captured["tradingPartnerServiceId"], "61101")
        self.assertEqual(fake.calls, ["Humana Medicare"])
        self.assertEqual(res.status, CoverageStatus.ACTIVE)

    def test_unresolved_name_falls_back_to_default(self):
        fake = FakePayers({})  # nothing resolves
        prov = self._provider(fake, payer_id="CMS")
        req = PatientRequest(first_name="Jane", last_name="Doe", dob="1980-01-01",
                             member_id="1EG4TE5MK72", payer_name="Some Unknown Plan")
        # Falls back to CMS default -> MBI-format gate runs (valid MBI here).
        captured = {}
        self._stub_post(prov, captured)
        res = prov.verify(req)
        self.assertEqual(captured["tradingPartnerServiceId"], "CMS")
        self.assertEqual(res.status, CoverageStatus.ACTIVE)

    def test_provider_name_required(self):
        prov = StediProvider(api_key="k", provider_npi="1999999984",
                             provider_name="", payers_client=FakePayers({}))
        req = PatientRequest(first_name="Jane", last_name="Doe", dob="1980-01-01",
                             member_id="W1", payer_id="60054")
        res = prov.verify(req)
        self.assertEqual(res.status, CoverageStatus.UNKNOWN)
        self.assertEqual(res.raw.get("reason"), "no_provider_name")

    def test_unconfigured_refuses(self):
        prov = StediProvider(api_key="", provider_npi="", provider_name="ACME")
        req = PatientRequest(first_name="J", last_name="D", dob="1980-01-01",
                             member_id="W1", payer_id="60054")
        res = prov.verify(req)
        self.assertEqual(res.status, CoverageStatus.UNKNOWN)
        self.assertIs(res.raw.get("configured"), False)

    def test_cms_invalid_mbi_not_sent(self):
        prov = self._provider(FakePayers({}), payer_id="CMS")
        sent = {"called": False}
        prov._post = lambda body: sent.update(called=True) or {}
        req = PatientRequest(first_name="Jane", last_name="Doe", dob="1980-01-01",
                             member_id="not-an-mbi", payer_id="CMS")
        res = prov.verify(req)
        self.assertEqual(res.status, CoverageStatus.UNKNOWN)
        self.assertEqual(res.raw.get("reason"), "invalid_mbi")
        self.assertFalse(sent["called"])  # never hit the network


if __name__ == "__main__":
    unittest.main(verbosity=2)
