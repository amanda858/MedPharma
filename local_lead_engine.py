#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Local Lead Engine for Existing Lab CSVs

What it does:
- Uses your existing labs_routed_top.csv as the primary source.
- Optionally uses labs_apollo_companies.csv (if present) to avoid duplicates.
- Auto-detects useful columns (volume, denials, payer issues, workflow flags, type/specialty, system size, etc.).
- Computes Money / Pain / Fit / Total scores based on whatever columns exist.
- Exports:
    - labs_scored_local.csv
    - labs_top_50_local.csv
    - labs_apollo_companies_enriched.csv

Usage:
    1) Put this file in the same folder as:
        - labs_routed_top.csv
        - (optional) labs_apollo_companies.csv
    2) Run: python local_lead_engine.py
"""

import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

ROUTED_FILE = os.path.join(BASE_DIR, "labs_routed_top.csv")
if not os.path.exists(ROUTED_FILE):
    for cand in [
        os.path.join(BASE_DIR, "output", "labs_routed_top.csv"),
        os.path.join(BASE_DIR, "output", "labs_routed_full.csv"),
        os.path.join(BASE_DIR, "output", "seed_routed.csv"),
    ]:
        if os.path.exists(cand):
            ROUTED_FILE = cand
            break
APOLLO_FILE = os.path.join(BASE_DIR, "labs_apollo_companies.csv")


# -------------------------------------------------------------------
# UTILITIES
# -------------------------------------------------------------------

def safe_read_csv(path: str) -> pd.DataFrame:
    print(f"[LOAD] {path}")
    if not os.path.exists(path):
        print(f"[WARN] File not found: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")
        return pd.DataFrame()


def normalize_str(s):
    if pd.isna(s):
        return ""
    return str(s).strip()


def contains_any(text: str, keywords) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)


def to_numeric_safe(series, default=0.0):
    return pd.to_numeric(series, errors="coerce").fillna(default)


# -------------------------------------------------------------------
# COLUMN DETECTION
# -------------------------------------------------------------------

def detect_column(df: pd.DataFrame, candidates):
    cols = [c.lower() for c in df.columns]
    mapping = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in mapping:
            return mapping[cand.lower()]
    for cand in candidates:
        for c in cols:
            if cand.lower() in c:
                return mapping[c]
    return None


def detect_schema(df: pd.DataFrame) -> dict:
    schema = {}

    schema["name"] = detect_column(df, [
        "org_name", "organization", "facility_name", "lab_name", "name", "company"
    ])
    schema["city"] = detect_column(df, ["city"])
    schema["state"] = detect_column(df, ["state", "st"])
    schema["zip"] = detect_column(df, ["zip", "zipcode", "postal"])
    schema["type"] = detect_column(df, [
        "type", "facility_type", "site_type", "specialty", "taxonomy"
    ])
    schema["volume"] = detect_column(df, [
        "volume", "claims", "encounters", "tests", "accessions"
    ])
    schema["denials"] = detect_column(df, [
        "denials", "denied", "denial_count", "denial_rate"
    ])
    schema["payer_issues"] = detect_column(df, [
        "payer_issue", "payer_issues", "payer_flag", "payer_problem", "payer_status"
    ])
    schema["workflow_flag"] = detect_column(df, [
        "workflow_issue", "workflow_flag", "bottleneck", "backlog", "turnaround_issue"
    ])
    schema["system_size"] = detect_column(df, [
        "system_size", "beds", "locations", "sites", "branches"
    ])
    schema["priority"] = detect_column(df, [
        "priority", "tier", "score", "rank"
    ])

    return schema


# -------------------------------------------------------------------
# SCORING
# -------------------------------------------------------------------

LAB_KEYWORDS = ["lab", "laboratory", "pathology", "toxicology", "molecular"]
URGENT_CARE_KEYWORDS = ["urgent care"]
ASC_KEYWORDS = ["asc", "ambulatory surgery", "ambulatory surgical center"]
HEALTH_SYSTEM_KEYWORDS = ["health system", "hospital", "medical center", "clinic system"]


def score_money(df: pd.DataFrame, schema: dict) -> pd.Series:
    score = pd.Series(0, index=df.index, dtype="int64")

    if schema["volume"]:
        vol = to_numeric_safe(df[schema["volume"]])
        score += pd.cut(
            vol,
            bins=[-1, 0, 100, 1000, 10000, float("inf")],
            labels=[0, 1, 2, 3, 4]
        ).astype(int)

    if schema["system_size"]:
        size = to_numeric_safe(df[schema["system_size"]])
        score += pd.cut(
            size,
            bins=[-1, 0, 10, 50, 200, float("inf")],
            labels=[0, 1, 2, 3, 4]
        ).astype(int)

    return score


def score_pain(df: pd.DataFrame, schema: dict) -> pd.Series:
    score = pd.Series(0, index=df.index, dtype="int64")

    if schema["denials"]:
        den = df[schema["denials"]].fillna("").astype(str).str.lower()
        den_num = to_numeric_safe(df[schema["denials"]])
        score += pd.cut(
            den_num,
            bins=[-1, 0, 10, 100, 1000, float("inf")],
            labels=[0, 1, 2, 3, 4]
        ).astype(int)
        score += den.str.contains("high|spike|issue|problem|backlog|appeal", na=False).astype(int)

    if schema["payer_issues"]:
        payer = df[schema["payer_issues"]].fillna("").astype(str).str.lower()
        score += 2 * payer.str.contains(
            "denial|not enrolled|out of network|rejected|pending|hold|suspension|under review",
            na=False
        ).astype(int)

    if schema["workflow_flag"]:
        wf = df[schema["workflow_flag"]].fillna("").astype(str).str.lower()
        score += 2 * wf.str.contains(
            "backlog|bottleneck|slow|delay|turnaround|manual|overwhelmed|capacity",
            na=False
        ).astype(int)

    return score


def score_fit(df: pd.DataFrame, schema: dict) -> pd.Series:
    score = pd.Series(0, index=df.index, dtype="int64")

    if schema["type"]:
        t = df[schema["type"]].fillna("").astype(str).str.lower()
        score += 3 * t.apply(lambda x: contains_any(x, LAB_KEYWORDS)).astype(int)
        score += 2 * t.apply(lambda x: contains_any(x, URGENT_CARE_KEYWORDS)).astype(int)
        score += 2 * t.apply(lambda x: contains_any(x, ASC_KEYWORDS)).astype(int)
        score += 2 * t.apply(lambda x: contains_any(x, HEALTH_SYSTEM_KEYWORDS)).astype(int)

    if schema["priority"]:
        pr = df[schema["priority"]].fillna("").astype(str).str.lower()
        score += pr.str.contains("high|tier 1|tier1|p1|hot", na=False).astype(int)

    return score


# -------------------------------------------------------------------
# APOLLO EXPORT
# -------------------------------------------------------------------

def build_apollo_export(df: pd.DataFrame, schema: dict, existing_apollo: pd.DataFrame) -> pd.DataFrame:
    name_col = schema["name"] or schema["type"] or schema["city"]
    if name_col is None:
        name_col = df.columns[0]

    out = pd.DataFrame()
    out["Company Name"] = df[name_col].fillna("").astype(str).str.strip()

    out["City"] = df[schema["city"]].fillna("").astype(str).str.strip() if schema["city"] else ""
    out["State"] = df[schema["state"]].fillna("").astype(str).str.strip() if schema["state"] else ""
    out["Zip"] = df[schema["zip"]].fillna("").astype(str).str.strip() if schema["zip"] else ""

    out["Money Score"] = df["money_score"]
    out["Pain Score"] = df["pain_score"]
    out["Fit Score"] = df["fit_score"]
    out["Total Score"] = df["total_score"]

    out["Target Roles"] = "Lab Director; Billing Manager; RCM Director; Compliance Officer; COO; CEO; Credentialing Specialist"

    if not existing_apollo.empty:
        existing_names = existing_apollo.iloc[:, 0].astype(str).str.lower().str.strip().unique()
        mask_new = ~out["Company Name"].str.lower().str.strip().isin(existing_names)
        out = out[mask_new]

    out = out.sort_values("Total Score", ascending=False)
    out = out.drop_duplicates(subset=["Company Name", "City", "State"])

    return out


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    routed = safe_read_csv(ROUTED_FILE)
    if routed.empty:
        print("[ERROR] labs_routed_top.csv is missing or empty. Put it next to this script and run again.")
        return

    apollo_existing = safe_read_csv(APOLLO_FILE)

    routed = routed.copy()
    for c in routed.columns:
        routed[c] = routed[c].map(normalize_str)

    schema = detect_schema(routed)
    print("[INFO] Detected schema:")
    for k, v in schema.items():
        print(f"   {k}: {v}")

    routed["money_score"] = score_money(routed, schema)
    routed["pain_score"] = score_pain(routed, schema)
    routed["fit_score"] = score_fit(routed, schema)
    routed["total_score"] = routed["money_score"] + routed["pain_score"] + routed["fit_score"]

    routed_scored = routed.sort_values("total_score", ascending=False).reset_index(drop=True)

    full_path = os.path.join(OUTPUT_DIR, "labs_scored_local.csv")
    routed_scored.to_csv(full_path, index=False)
    print(f"[OUT] Full scored leads: {full_path}")

    top50 = routed_scored.head(50)
    top50_path = os.path.join(OUTPUT_DIR, "labs_top_50_local.csv")
    top50.to_csv(top50_path, index=False)
    print(f"[OUT] Top 50 leads: {top50_path}")

    apollo_export = build_apollo_export(routed_scored, schema, apollo_existing)
    apollo_export_path = os.path.join(OUTPUT_DIR, "labs_apollo_companies_enriched.csv")
    apollo_export.to_csv(apollo_export_path, index=False)
    print(f"[OUT] Apollo company export: {apollo_export_path}")

    print("\n[NOTE] Next steps:")
    print("  1) Open labs_scored_local.csv to see how your existing leads scored.")
    print("  2) Use labs_top_50_local.csv as your immediate outbound focus list.")
    print("  3) Upload labs_apollo_companies_enriched.csv into Apollo and pull contacts by role.")


if __name__ == "__main__":
    main()
