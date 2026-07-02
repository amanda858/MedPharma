"""Batch Excel review demo:  python3 -m eligibility_hybrid.demo_batch

Proves the "upload a spreadsheet, review everyone at once" workflow:
  1. writes a sample patient roster to a real .xlsx (as if uploaded),
  2. parses it back the way the hub would,
  3. runs every row through eligibility + medical necessity + prior auth,
  4. writes a color-coded reviewed .xlsx and prints a summary.
"""
from __future__ import annotations

import os
import tempfile

import openpyxl

from .batch import (read_rows, review_rows, summarize, write_review_csv,
                    write_review_xlsx)

INPUT_HEADERS = ["First", "Last", "DOB", "Payer", "Member ID", "SSN", "ZIP",
                 "CPT", "ICD", "NPI"]

INPUT_ROWS = [
    ["Marcus", "Bell", "1974-03-12", "UnitedHealthcare", "912345678", "", "", "87631;87635", "J12.81;R05.9", "1972000000"],
    ["Deja", "Franklin", "1991-06-02", "", "", "4821", "33101", "87507;87798", "A08.4;N39.0", "1972000000"],
    ["Kevin", "ONeil", "1988-11-19", "Cigna", "U8842019", "", "", "87631", "J20.9", "1972000000"],
    ["Nadia", "Cole", "1969-09-09", "Aetna", "W5567781", "", "", "87631", "Z00.00", "1972000000"],
    ["Harold", "Metz", "1948-02-14", "Medicare Part B", "1EG4TE5MK72", "", "", "87631", "Z00.00", "1972000000"],
    ["Sara", "Kim", "1995-01-30", "Aetna", "W7781234", "", "", "87798", "N39.0", "1972000000"],
]


def _make_sample_xlsx(path: str) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Accessions"
    ws.append(INPUT_HEADERS)
    for r in INPUT_ROWS:
        ws.append(r)
    wb.save(path)


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="elig_batch_")
    in_path = os.path.join(tmp, "patient_roster.xlsx")
    out_xlsx = os.path.join(tmp, "coverage_review.xlsx")
    out_csv = os.path.join(tmp, "coverage_review.csv")

    _make_sample_xlsx(in_path)
    rows = read_rows(in_path)                     # parse like an upload
    reviewed = review_rows(rows)                  # the brain
    write_review_xlsx(out_xlsx, reviewed)
    write_review_csv(out_csv, reviewed)

    print("\nMedPharma Batch Coverage Review — SANDBOX")
    print(f"Uploaded {len(rows)} patients -> {len(reviewed)} reviewed test lines\n")
    hdr = f"{'PATIENT':<16}{'CPT':<8}{'DISPOSITION':<26}{'COVERAGE':<10}{'PT $':>7}{'EV $':>9}"
    print(hdr)
    print("-" * len(hdr))
    for row in reviewed:
        pt = row["Patient"]
        cpt = row["CPT"]
        disp = row["Disposition"]
        cov = row["Coverage"]
        prc = row["Patient $"]
        ev = row["Expected $"]
        prc_s = f"{prc:.0f}" if isinstance(prc, (int, float)) else ""
        ev_s = f"{ev:.2f}" if isinstance(ev, (int, float)) else ""
        print(f"{pt:<16}{cpt:<8}{disp:<26}{cov:<10}{prc_s:>7}{ev_s:>9}")

    print("\nSummary:")
    for disp, n in sorted(summarize(reviewed).items(), key=lambda kv: -kv[1]):
        print(f"  {n:>2}x  {disp}")
    total_ev = sum(r["Expected $"] for r in reviewed if isinstance(r["Expected $"], (int, float)))
    print(f"\nPortfolio expected value: ${total_ev:,.2f}")
    print(f"\nReviewed workbook : {out_xlsx}")
    print(f"Reviewed CSV      : {out_csv}")


if __name__ == "__main__":
    main()
