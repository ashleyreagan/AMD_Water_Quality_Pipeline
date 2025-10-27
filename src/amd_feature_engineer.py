#!/usr/bin/env python3
"""
AMD Feature Engineering – v1.2 (fault-tolerant)
Generates per-site AMD features for ML from the AMD-subset CSV.
Handles malformed CSV rows, coerces numerics, and drops sentinel outliers.
"""

import pandas as pd
import numpy as np
from scipy.stats import linregress
import argparse, os, sys

AMD_PARAMS = ["pH", "Iron", "Manganese", "Aluminum", "Sulfate", "Alkalinity", "Specific conductance"]
USECOLS = [
    "MonitoringLocationIdentifier",
    "CharacteristicName",
    "ActivityStartDate",
    "ResultMeasureValue"
]

def safe_read_csv(path: str) -> pd.DataFrame:
    # Try given path; if missing, also look under data_raw/chemistry/
    if not os.path.exists(path):
        alt = os.path.join("data_raw", "chemistry", os.path.basename(path))
        if os.path.exists(alt):
            print(f"📂 Using fallback path: {alt}")
            path = alt
        else:
            print(f"❌ File not found: {path}")
            sys.exit(1)

    print(f"📥 Loading {os.path.basename(path)} (fault-tolerant reader)…")
    df = pd.read_csv(
        path,
        usecols=lambda c: c in USECOLS,  # only the columns we need
        low_memory=False,
        on_bad_lines="skip",
        encoding="utf-8"
    )
    return df

def build_features(input_path: str):
    print(f"\n🧠 Building ML feature matrix from {os.path.basename(input_path)} …")

    df = safe_read_csv(input_path)

    # Basic parse & filter
    df["ActivityStartDate"] = pd.to_datetime(df["ActivityStartDate"], errors="coerce")
    df["Year"] = df["ActivityStartDate"].dt.year
    df["ResultMeasureValue"] = pd.to_numeric(df["ResultMeasureValue"], errors="coerce")
    df = df.dropna(subset=["Year", "ResultMeasureValue", "CharacteristicName", "MonitoringLocationIdentifier"])

    # Keep AMD parameters only
    df = df[df["CharacteristicName"].isin(AMD_PARAMS)]

    # Remove obvious sentinel/outlier values often present in WQP exports
    # (999999 etc.). Keep legitimate negatives (e.g., alkalinity can be < 0).
    df.loc[df["ResultMeasureValue"].abs() > 1e5, "ResultMeasureValue"] = np.nan
    df = df.dropna(subset=["ResultMeasureValue"])

    print(f"✅ Rows after cleaning/filtering: {len(df):,}")

    # Aggregate mean by site × parameter × year
    grouped = (
        df.groupby(["MonitoringLocationIdentifier", "CharacteristicName", "Year"])["ResultMeasureValue"]
          .mean()
          .reset_index()
    )

    features = []
    for site, g_site in grouped.groupby("MonitoringLocationIdentifier"):
        site_feats = {"MonitoringLocationIdentifier": site}

        for param, g_param in g_site.groupby("CharacteristicName"):
            vals = g_param["ResultMeasureValue"].astype(float)
            years = g_param["Year"].astype(int)

            # Basic stats
            site_feats[f"{param}_mean"]   = vals.mean()
            site_feats[f"{param}_median"] = vals.median()
            site_feats[f"{param}_std"]    = vals.std(ddof=0)
            site_feats[f"{param}_min"]    = vals.min()
            site_feats[f"{param}_max"]    = vals.max()
            site_feats[f"{param}_n_years"] = years.nunique()

            # Trend (only if we have > 2 distinct years)
            if site_feats[f"{param}_n_years"] > 2:
                slope, _, _, _, _ = linregress(years, vals)
                site_feats[f"{param}_trend"] = slope
            else:
                site_feats[f"{param}_trend"] = np.nan

        features.append(site_feats)

    feat_df = pd.DataFrame(features)

    # Derived ratios (guard against divide-by-zero)
    def safe_div(a, b):
        return np.where((b is None) or (b == 0), np.nan, a / b)

    if "Iron_mean" in feat_df and "Manganese_mean" in feat_df:
        feat_df["Fe_Mn_ratio"] = feat_df["Iron_mean"] / feat_df["Manganese_mean"]
    else:
        feat_df["Fe_Mn_ratio"] = np.nan

    if "Iron_mean" in feat_df and "Sulfate_mean" in feat_df:
        feat_df["Fe_Sulfate_ratio"] = feat_df["Iron_mean"] / feat_df["Sulfate_mean"]
    else:
        feat_df["Fe_Sulfate_ratio"] = np.nan

    # AMD impact label (simple initial rule; can be refined later)
    feat_df["AMD_impacted"] = np.where(
        (feat_df.get("pH_mean", np.nan) < 5) | (feat_df.get("Iron_mean", np.nan) > 10),
        1, 0
    )

    # Output path mirrors input, replacing suffix
    out_path = os.path.splitext(input_path)[0].replace("_AMDsubset", "_AMD_features.csv")
    # If input wasn’t the AMDsubset name, default next to input filename
    if out_path == os.path.splitext(input_path)[0]:
        out_path = os.path.splitext(input_path)[0] + "_AMD_features.csv"

    feat_df.to_csv(out_path, index=False)
    print(f"💾 Feature table saved → {out_path}")
    print(f"📊 Sites: {feat_df.shape[0]} | Features: {feat_df.shape[1]}")

    # Quick sanity print
    cols_show = [c for c in ["pH_mean", "Iron_mean", "Sulfate_mean", "Fe_Mn_ratio", "Fe_Sulfate_ratio", "AMD_impacted"] if c in feat_df.columns]
    print("\n🔎 Sample:")
    print(feat_df[["MonitoringLocationIdentifier"] + cols_show].head(8).to_string(index=False))

    return feat_df

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate AMD ML feature matrix from WQX AMD subset (robust).")
    ap.add_argument("--input", required=True, help="Path to AMD subset CSV (e.g., wqx_pa_sites_merged_AMDsubset.csv)")
    args = ap.parse_args()
    build_features(args.input)