"""Proof-of-correctness tests for MedPharma Coverage Intercept.

Runs the REAL engine (sandbox) over named correct-coding scenarios and asserts
each claim-integrity rule fires exactly when it should — and stays quiet when it
shouldn't (no false positives). Every scenario maps to a public standard.

Run directly:   python3 test_coverage_intercept.py
Or under pytest: pytest test_coverage_intercept.py
"""
from eligibility_hybrid import (PRODUCT_NAME, METHOD, PatientRequest,
                                run_intercept, summarize_findings)
from eligibility_hybrid.batch import build_review_gate

_GATE = build_review_gate()


def _evaluate(**kw):
    """Build a PatientRequest, run the full gate, return its dict form."""
    req = PatientRequest(**kw)
    return _GATE.evaluate(req).to_dict()


def _codes(result):
    return [f["code"] for f in result["integrity"]]


# ── Rule fires when it should ──────────────────────────────────────────────

def test_ncci_ptp_bundling_fires():
    """NCCI PTP: a single-target assay billed with the panel that subsumes it."""
    r = _evaluate(first_name="Marcus", last_name="Bell", dob="1979-04-12",
                  payer_name="UnitedHealthcare", member_id="W2000001",
                  cpt_codes=["87631", "87635"], icd10_codes=["J12.81", "R05.9"],
                  provider_npi="1234567890")
    ncci = [f for f in r["integrity"] if f["code"] == "NCCI_PTP"]
    assert ncci, f"expected NCCI_PTP, got {_codes(r)}"
    assert ncci[0]["cpt"] == "87635" and "87631" in ncci[0]["related"]
    assert ncci[0]["severity"] == "warn"


def test_mue_duplicate_units_fires():
    """MUE: the same single-unit panel ordered twice on one date of service."""
    r = _evaluate(first_name="Dana", last_name="Cole", dob="1985-05-05",
                  payer_name="Aetna", member_id="W7000002",
                  cpt_codes=["87631", "87631"], icd10_codes=["J12.81"],
                  provider_npi="1234567890")
    assert "MUE_EXCEEDED" in _codes(r), f"expected MUE_EXCEEDED, got {_codes(r)}"


def test_qw_modifier_medicare_fires():
    """QW: a CLIA-waived test billed to traditional Medicare needs the QW modifier."""
    r = _evaluate(first_name="Nora", last_name="Diaz", dob="1950-03-03",
                  payer_name="Medicare Part B", member_id="1EG4TE5MK72",
                  cpt_codes=["87635"], icd10_codes=["U07.1"],
                  provider_npi="1234567890")
    assert "QW_MODIFIER" in _codes(r), f"expected QW_MODIFIER, got {_codes(r)}"


def test_moldx_zcode_medicare_fires():
    """MolDX: a molecular test under Medicare needs DEX Z-Code registration."""
    r = _evaluate(first_name="Harold", last_name="Reed", dob="1948-02-02",
                  payer_name="Medicare Part B", member_id="1EG4TE5MK72",
                  cpt_codes=["81479"], icd10_codes=["C34.90"],
                  provider_npi="1234567890")
    assert "MOLDX_ZCODE" in _codes(r), f"expected MOLDX_ZCODE, got {_codes(r)}"


def test_termination_fires():
    """As-of-DOS termination: the mock terms members whose ID starts with U88."""
    r = _evaluate(first_name="Kevin", last_name="ONeil", dob="1965-02-20",
                  payer_name="Cigna", member_id="U8830012",
                  cpt_codes=["87631"], icd10_codes=["J12.81"],
                  provider_npi="1234567890")
    assert "COVERAGE_TERMED" in _codes(r), f"expected COVERAGE_TERMED, got {_codes(r)}"


# ── Rule stays quiet when it should (no false positives) ───────────────────

def test_clean_single_test_has_no_integrity_flags():
    """A single covered commercial test with a supporting Dx trips nothing."""
    r = _evaluate(first_name="Grace", last_name="Kim", dob="1990-09-09",
                  payer_name="UnitedHealthcare", member_id="W3000003",
                  cpt_codes=["87635"], icd10_codes=["U07.1"],
                  provider_npi="1234567890")
    assert r["integrity"] == [], f"expected no findings, got {_codes(r)}"


def test_ncci_does_not_fire_without_the_panel():
    """87635 alone (no 87631/87633 present) must NOT trigger a bundling edit."""
    r = _evaluate(first_name="Iris", last_name="Lane", dob="1975-07-07",
                  payer_name="Aetna", member_id="W4000004",
                  cpt_codes=["87635"], icd10_codes=["U07.1"],
                  provider_npi="1234567890")
    assert "NCCI_PTP" not in _codes(r)


def test_qw_does_not_fire_for_commercial():
    """QW is a Medicare rule — a commercial payer must not trigger it."""
    r = _evaluate(first_name="Owen", last_name="Park", dob="1982-08-08",
                  payer_name="Cigna", member_id="W5000005",
                  cpt_codes=["87635"], icd10_codes=["U07.1"],
                  provider_npi="1234567890")
    assert "QW_MODIFIER" not in _codes(r)


# ── Product + provenance guarantees ────────────────────────────────────────

def test_every_finding_is_explainable_and_cited():
    """Each finding carries a code, severity, message, action, and PUBLIC basis."""
    r = _evaluate(first_name="Harold", last_name="Reed", dob="1948-02-02",
                  payer_name="Medicare Part B", member_id="1EG4TE5MK72",
                  cpt_codes=["81479", "80305"], icd10_codes=["C34.90"],
                  provider_npi="1234567890")
    assert r["integrity"], "expected findings for this Medicare molecular case"
    for f in r["integrity"]:
        assert f["code"] and f["severity"] in ("info", "advisory", "warn", "block")
        assert f["message"] and f["basis"], f"finding not cited: {f}"
        assert f["method"] == METHOD == "medpharma-intercept"


def test_product_identity():
    assert PRODUCT_NAME == "MedPharma Coverage Intercept"


def test_dispositions_unchanged_by_intercept():
    """Regression pin: the intercept layer is additive — dispositions/EV hold."""
    r = _evaluate(first_name="Marcus", last_name="Bell", dob="1979-04-12",
                  payer_name="UnitedHealthcare", member_id="W2000001",
                  cpt_codes=["87631", "87635"], icd10_codes=["J12.81", "R05.9"],
                  provider_npi="1234567890")
    assert r["overall"] == "CLEAR TO RUN", r["overall"]
    assert isinstance(r["total_expected_value"], (int, float))
    # per-line integrity is attached to the right CPT
    line = next(l for l in r["lines"] if l["cpt"] == "87635")
    assert any(f["code"] == "NCCI_PTP" for f in line["integrity"])


def test_summary_counts_by_severity():
    r = _evaluate(first_name="Harold", last_name="Reed", dob="1948-02-02",
                  payer_name="Medicare Part B", member_id="1EG4TE5MK72",
                  cpt_codes=["81479", "80305"], icd10_codes=["C34.90"],
                  provider_npi="1234567890")
    s = summarize_findings(r["integrity"])
    assert s["total"] == len(r["integrity"]) and s["total"] >= 1


# ── Plain-script runner (no pytest needed) ─────────────────────────────────
if __name__ == "__main__":
    import sys
    tests = sorted((n, f) for n, f in globals().items()
                   if n.startswith("test_") and callable(f))
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  \u2713 {name}")
            passed += 1
        except Exception as e:  # noqa: BLE001 - report every failure
            print(f"  \u2717 {name}  ->  {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed  ({PRODUCT_NAME})")
    sys.exit(1 if failed else 0)
