#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Healthcare Lab Lead Engine (NPI + CLIA + State + CMS Denials)

One-file pipeline:
1. Load raw datasets (NPI, CLIA, state licensing, CMS denials).
2. Normalize and filter to labs / urgent care / ASC / health systems.
3. Merge into a unified facility table.
4. Compute Money / Pain / Fit scores.
5. Export scored leads and Apollo-ready enrichment file.

Requirements:
    pip install pandas python-dateutil

Run:
    python lab_lead_engine.py
"""

import os
import pandas as pd
from dateutil import parser as dateparser

# -------------------------------------------------------------------
# 0. CONFIG
# -------------------------------------------------------------------

OUTPUT_DIR = "./output"

# Replace these with your actual sources (local paths or HTTP URLs)
CONFIG = {
    "npi_source": "data/npi_registry.csv",          # or "https://..."
    "clia_source": "data/clia_labs.csv",
    "state_license_source": "data/state_labs.csv",
    "cms_denials_source": "data/cms_denials.csv",
}

# Taxonomy / specialty filters for labs, urgent care, ASC, health systems
LAB_TAXONOMY_KEYWORDS = [
    "laboratory", "clinical medical laboratory", "pathology", "toxicology",
    "molecular", "reference laboratory"
]

URGENT_CARE_KEYWORDS = ["urgent care"]
ASC_KEYWORDS = ["ambulatory surgical center", "asc"]
HEALTH_SYSTEM_KEYWORDS = ["hospital", "health system", "medical center"]

# -------------------------------------------------------------------
# 1. UTILITIES
# -------------------------------------------------------------------

def safe_read_csv(path_or_url: str) -> pd.DataFrame:
    print(f"[LOAD] {path_or_url}")
    try:
        return pd.read_csv(path_or_url, dtype=str, low_memory=False)
    except Exception as e:
        print(f"[WARN] Failed to read {path_or_url}: {e}")
        return pd.DataFrame()


def normalize_str(s):
    if pd.isna(s):
        return ""
    return str(s).strip()


def contains_any(text, keywords):
    t = text.lower()
    return any(k.lower() in t for k in keywords)


def parse_date_safe(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        return dateparser.parse(str(val), fuzzy=True)
    except Exception:
        return None


# -------------------------------------------------------------------
# 2. LOADERS
# -------------------------------------------------------------------

def load_npi(path_or_url: str) -> pd.DataFrame:
    df = safe_read_csv(path_or_url)
    if df.empty:
        return df

    col_map_candidates = {
        "NPI": ["NPI", "npi"],
        "ORG_NAME": [
            "Provider Organization Name (Legal Business Name)",
            "Provider Organization Name",
            "Organization Name",
            "org_name"
        ],
        "ADDR1": [
            "Provider First Line Business Practice Location Address",
            "Practice Address Line 1",
            "address1"
        ],
        "CITY": [
            "Provider Business Practice Location Address City Name",
            "Practice City",
            "city"
        ],
        "STATE": [
            "Provider Business Practice Location Address State Name",
            "Provider Business Practice Location Address State",
            "Practice State",
            "state"
        ],
        "ZIP": [
            "Provider Business Practice Location Address Postal Code",
            "Practice Zip",
            "zip"
        ],
        "TAXONOMY": [
            "Healthcare Provider Taxonomy Description_1",
            "Healthcare Provider Taxonomy Code_1",
            "Primary Taxonomy",
            "taxonomy"
        ]
    }

    def find_col(df_cols, candidates):
        for c in candidates:
            if c in df_cols:
                return c
        return None

    cols = df.columns
    mapped = {k: find_col(cols, v) for k, v in col_map_candidates.items()}

    if "Entity Type Code" in cols:
        df = df[df["Entity Type Code"].astype(str) == "2"]

    df_out = pd.DataFrame()
    df_out["npi"] = df.get(mapped["NPI"], pd.Series(dtype=str)).astype(str)
    df_out["org_name"] = df.get(mapped["ORG_NAME"], "").astype(str).map(normalize_str)
    df_out["addr1"] = df.get(mapped["ADDR1"], "").astype(str).map(normalize_str)
    df_out["city"] = df.get(mapped["CITY"], "").astype(str).map(normalize_str)
    df_out["state"] = df.get(mapped["STATE"], "").astype(str).map(normalize_str)
    df_out["zip"] = df.get(mapped["ZIP"], "").astype(str).map(normalize_str)
    df_out["taxonomy"] = df.get(mapped["TAXONOMY"], "").astype(str).map(normalize_str)

    mask = (
        df_out["taxonomy"].str.lower().apply(
            lambda t: contains_any(t, LAB_TAXONOMY_KEYWORDS + URGENT_CARE_KEYWORDS + ASC_KEYWORDS)
        )
    )
    df_out = df_out[mask].reset_index(drop=True)

    df_out["source_npi"] = True
    return df_out


def load_clia(path_or_url: str) -> pd.DataFrame:
    df = safe_read_csv(path_or_url)
    if df.empty:
        return df

    col_map_candidates = {
        "CLIA": ["CLIA", "clia_number", "CLIA Number"],
        "LAB_NAME": ["Lab Name", "Laboratory Name", "lab_name"],
        "ADDR1": ["Address 1", "Street Address", "addr1"],
        "CITY": ["City", "city"],
        "STATE": ["State", "state"],
        "ZIP": ["Zip", "ZIP", "zip"],
        "LAB_TYPE": ["Lab Type", "Type", "lab_type"],
        "COMPLEXITY": ["Complexity", "complexity"],
    }

    def find_col(df_cols, candidates):
        for c in candidates:
            if c in df_cols:
                return c
        return None

    cols = df.columns
    mapped = {k: find_col(cols, v) for k, v in col_map_candidates.items()}

    df_out = pd.DataFrame()
    df_out["clia"] = df.get(mapped["CLIA"], "").astype(str).map(normalize_str)
    df_out["lab_name"] = df.get(mapped["LAB_NAME"], "").astype(str).map(normalize_str)
    df_out["addr1"] = df.get(mapped["ADDR1"], "").astype(str).map(normalize_str)
    df_out["city"] = df.get(mapped["CITY"], "").astype(str).map(normalize_str)
    df_out["state"] = df.get(mapped["STATE"], "").astype(str).map(normalize_str)
    df_out["zip"] = df.get(mapped["ZIP"], "").astype(str).map(normalize_str)
    df_out["lab_type"] = df.get(mapped["LAB_TYPE"], "").astype(str).map(normalize_str)
    df_out["complexity"] = df.get(mapped["COMPLEXITY"], "").astype(str).map(normalize_str)

    df_out["source_clia"] = True
    return df_out


def load_state_license(path_or_url: str) -> pd.DataFrame:
    df = safe_read_csv(path_or_url)
    if df.empty:
        return df

    col_map_candidates = {
        "LICENSE_ID": ["License Number", "License ID", "license_id"],
        "FACILITY_NAME": ["Facility Name", "Business Name", "facility_name"],
        "ADDR1": ["Address 1", "Street Address", "addr1"],
        "CITY": ["City", "city"],
        "STATE": ["State", "state"],
        "ZIP": ["Zip", "ZIP", "zip"],
        "LICENSE_STATUS": ["Status", "License Status", "status"],
        "LICENSE_TYPE": ["License Type", "Type", "license_type"],
        "ISSUE_DATE": ["Issue Date", "issue_date"],
        "EXPIRY_DATE": ["Expiration Date", "Expiry Date", "expiry_date"],
    }

    def find_col(df_cols, candidates):
        for c in candidates:
            if c in df_cols:
                return c
        return None

    cols = df.columns
    mapped = {k: find_col(cols, v) for k, v in col_map_candidates.items()}

    df_out = pd.DataFrame()
    df_out["license_id"] = df.get(mapped["LICENSE_ID"], "").astype(str).map(normalize_str)
    df_out["facility_name"] = df.get(mapped["FACILITY_NAME"], "").astype(str).map(normalize_str)
    df_out["addr1"] = df.get(mapped["ADDR1"], "").astype(str).map(normalize_str)
    df_out["city"] = df.get(mapped["CITY"], "").astype(str).map(normalize_str)
    df_out["state"] = df.get(mapped["STATE"], "").astype(str).map(normalize_str)
    df_out["zip"] = df.get(mapped["ZIP"], "").astype(str).map(normalize_str)
    df_out["license_status"] = df.get(mapped["LICENSE_STATUS"], "").astype(str).map(normalize_str)
    df_out["license_type"] = df.get(mapped["LICENSE_TYPE"], "").astype(str).map(normalize_str)

    df_out["issue_date"] = df.get(mapped["ISSUE_DATE"], "").map(parse_date_safe)
    df_out["expiry_date"] = df.get(mapped["EXPIRY_DATE"], "").map(parse_date_safe)

    df_out["source_state"] = True
    return df_out


def load_cms_denials(path_or_url: str) -> pd.DataFrame:
    df = safe_read_csv(path_or_url)
    if df.empty:
        return df

    col_map_candidates = {
        "NPI": ["NPI", "npi"],
        "CLIA": ["CLIA", "clia"],
        "TOTAL_CLAIMS": ["Total Claims", "total_claims"],
        "DENIED_CLAIMS": ["Denied Claims", "denied_claims"],
        "DENIAL_AMOUNT": ["Denied Amount", "denied_amount"],
        "PERIOD": ["Period", "period"],
    }

    def find_col(df_cols, candidates):
        for c in candidates:
            if c in df_cols:
                return c
        return None

    cols = df.columns
    mapped = {k: find_col(cols, v) for k, v in col_map_candidates.items()}

    df_out = pd.DataFrame()
    df_out["npi"] = df.get(mapped["NPI"], "").astype(str).map(normalize_str)
    df_out["clia"] = df.get(mapped["CLIA"], "").astype(str).map(normalize_str)
    df_out["total_claims"] = pd.to_numeric(df.get(mapped["TOTAL_CLAIMS"], 0), errors="coerce").fillna(0)
    df_out["denied_claims"] = pd.to_numeric(df.get(mapped["DENIED_CLAIMS"], 0), errors="coerce").fillna(0)
    df_out["denied_amount"] = pd.to_numeric(df.get(mapped["DENIAL_AMOUNT"], 0), errors="coerce").fillna(0)
    df_out["period"] = df.get(mapped["PERIOD"], "").astype(str).map(normalize_str)

    df_out["denial_rate"] = df_out.apply(
        lambda r: (r["denied_claims"] / r["total_claims"]) if r["total_claims"] > 0 else 0,
        axis=1
    )

    df_out["source_cms_denials"] = True
    return df_out


# -------------------------------------------------------------------
# 3. MERGE & SCORING
# -------------------------------------------------------------------

def merge_facilities(npi_df, clia_df, state_df, denials_df) -> pd.DataFrame:
    base = npi_df.copy()

    if not clia_df.empty:
        clia_df_key = clia_df.copy()
        clia_df_key["merge_key"] = (
            clia_df_key["city"].str.lower().fillna("") + "|" +
            clia_df_key["state"].str.lower().fillna("") + "|" +
            clia_df_key["zip"].str[:5].fillna("")
        )
        base["merge_key"] = (
            base["city"].str.lower().fillna("") + "|" +
            base["state"].str.lower().fillna("") + "|" +
            base["zip"].str[:5].fillna("")
        )
        base = base.merge(
            clia_df_key.drop_duplicates(subset=["merge_key"]),
            on="merge_key",
            how="left",
            suffixes=("", "_clia")
        )

    if not state_df.empty:
        state_df_key = state_df.copy()
        state_df_key["merge_key2"] = (
            state_df_key["city"].str.lower().fillna("") + "|" +
            state_df_key["state"].str.lower().fillna("") + "|" +
            state_df_key["zip"].str[:5].fillna("")
        )
        base["merge_key2"] = (
            base["city"].str.lower().fillna("") + "|" +
            base["state"].str.lower().fillna("") + "|" +
            base["zip"].str[:5].fillna("")
        )
        base = base.merge(
            state_df_key.drop_duplicates(subset=["merge_key2"]),
            left_on="merge_key2",
            right_on="merge_key2",
            how="left",
            suffixes=("", "_state")
        )

    if not denials_df.empty:
        base = base.merge(
            denials_df.drop_duplicates(subset=["npi"]),
            on="npi",
            how="left",
            suffixes=("", "_denial")
        )

    for col in ["merge_key", "merge_key2"]:
        if col in base.columns:
            base.drop(columns=[col], inplace=True)

    return base


def score_facilities(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def _col(name, default=""):
        if name in df.columns:
            return df[name]
        return pd.Series([default] * len(df), index=df.index)

    df["money_score"] = 0
    df.loc[_col("complexity").astype(str).str.lower().str.contains("high", na=False), "money_score"] += 3
    df.loc[_col("clia").astype(str).str.len() > 0, "money_score"] += 2
    df["money_score"] += pd.cut(
        pd.to_numeric(_col("total_claims", 0), errors="coerce").fillna(0),
        bins=[-1, 0, 1000, 10000, 100000, float("inf")],
        labels=[0, 1, 2, 3, 4]
    ).astype(int)

    df["pain_score"] = 0
    df["pain_score"] += pd.cut(
        pd.to_numeric(_col("denial_rate", 0.0), errors="coerce").fillna(0.0),
        bins=[-0.01, 0.05, 0.10, 0.20, 1.0],
        labels=[0, 1, 2, 3]
    ).astype(int)
    df["pain_score"] += pd.cut(
        pd.to_numeric(_col("denied_amount", 0.0), errors="coerce").fillna(0.0),
        bins=[-1, 0, 10000, 100000, 1000000, float("inf")],
        labels=[0, 1, 2, 3, 4]
    ).astype(int)
    license_status = _col("license_status").astype(str).str.lower()
    df.loc[license_status.str.contains("probation|suspend|revok|discipline|conditional", na=False), "pain_score"] += 3

    df["fit_score"] = 0
    df.loc[
        _col("taxonomy").astype(str).str.lower().apply(lambda t: contains_any(t, LAB_TAXONOMY_KEYWORDS)),
        "fit_score"
    ] += 3
    df.loc[
        _col("taxonomy").astype(str).str.lower().apply(lambda t: contains_any(t, URGENT_CARE_KEYWORDS)),
        "fit_score"
    ] += 2
    df.loc[
        _col("taxonomy").astype(str).str.lower().apply(lambda t: contains_any(t, ASC_KEYWORDS)),
        "fit_score"
    ] += 2
    df.loc[
        _col("org_name").astype(str).str.lower().apply(lambda t: contains_any(t, HEALTH_SYSTEM_KEYWORDS)),
        "fit_score"
    ] += 2

    df["total_score"] = df["money_score"] + df["pain_score"] + df["fit_score"]
    return df


# -------------------------------------------------------------------
# 4. APOLLO / OUTREACH EXPORT
# -------------------------------------------------------------------

def build_apollo_export(df: pd.DataFrame) -> pd.DataFrame:
    export_cols = [
        "org_name", "lab_name", "facility_name",
        "addr1", "city", "state", "zip",
        "npi", "clia", "license_id",
        "money_score", "pain_score", "fit_score", "total_score"
    ]

    for c in export_cols:
        if c not in df.columns:
            df[c] = ""

    out = df[export_cols].copy()

    out["company_name"] = out["org_name"]
    out.loc[out["company_name"] == "", "company_name"] = out["lab_name"]
    out.loc[out["company_name"] == "", "company_name"] = out["facility_name"]

    out["company_key"] = (
        out["company_name"].astype(str).str.lower().fillna("") + "|" +
        out["city"].astype(str).str.lower().fillna("") + "|" +
        out["state"].astype(str).str.lower().fillna("")
    )
    out = out.sort_values("total_score", ascending=False)
    out = out.drop_duplicates(subset=["company_key"])

    apollo = pd.DataFrame()
    apollo["Company Name"] = out["company_name"]
    apollo["City"] = out["city"]
    apollo["State"] = out["state"]
    apollo["Zip"] = out["zip"]
    apollo["NPI"] = out["npi"]
    apollo["CLIA"] = out["clia"]
    apollo["License ID"] = out["license_id"]
    apollo["Money Score"] = out["money_score"]
    apollo["Pain Score"] = out["pain_score"]
    apollo["Fit Score"] = out["fit_score"]
    apollo["Total Score"] = out["total_score"]
    apollo["Target Roles"] = "Lab Director; Billing Manager; RCM Director; Compliance Officer; COO; CEO; Credentialing Specialist"

    return apollo


# -------------------------------------------------------------------
# 5. MAIN
# -------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    npi_df = load_npi(CONFIG["npi_source"])
    clia_df = load_clia(CONFIG["clia_source"])
    state_df = load_state_license(CONFIG["state_license_source"])
    denials_df = load_cms_denials(CONFIG["cms_denials_source"])

    print(f"[INFO] NPI rows: {len(npi_df)}")
    print(f"[INFO] CLIA rows: {len(clia_df)}")
    print(f"[INFO] State license rows: {len(state_df)}")
    print(f"[INFO] CMS denials rows: {len(denials_df)}")

    merged = merge_facilities(npi_df, clia_df, state_df, denials_df)
    print(f"[INFO] Merged rows: {len(merged)}")

    scored = score_facilities(merged)
    scored = scored.sort_values("total_score", ascending=False).reset_index(drop=True)

    full_path = os.path.join(OUTPUT_DIR, "labs_scored.csv")
    scored.to_csv(full_path, index=False)
    print(f"[OUT] Full scored labs: {full_path}")

    top50 = scored.head(50)
    top50_path = os.path.join(OUTPUT_DIR, "labs_top_50.csv")
    top50.to_csv(top50_path, index=False)
    print(f"[OUT] Top 50 labs: {top50_path}")

    apollo = build_apollo_export(scored)
    apollo_path = os.path.join(OUTPUT_DIR, "labs_apollo_companies.csv")
    apollo.to_csv(apollo_path, index=False)
    print(f"[OUT] Apollo company export: {apollo_path}")

    print("\n[NOTE] Next steps:")
    print("  1) Plug real CSV URLs/paths into CONFIG.")
    print("  2) Run this script to generate scored leads.")
    print("  3) Upload labs_apollo_companies.csv into Apollo.")
    print("  4) Use Apollo to pull contacts by role for outbound sequences.")


if __name__ == "__main__":
    main()
