#!/usr/bin/env python3
"""
AMD Profile Summary – v2.0
Author: Ashley Mitchell, OSMRE 2025

Robust summary generator for WQX AMD datasets.
Now includes fault-tolerant CSV parsing, automatic path search, and progress feedback.
"""

import pandas as pd
import argparse
import os
import sys

def summarize_wqx(file_path):
    # ---- Resolve path ----
    if not os.path.exists(file_path):
        alt_path = os.path.join("data_raw", "chemistry", os.path.basename(file_path))
        if os.path.exists(alt_path):
            print(f"📂 File not found at {file_path}, using {alt_path}")
            file_path = alt_path
        else:
            print(f"❌ File not found in either location:\n   - {file_path}\n   - {alt_path}")
            sys.exit(1)

    print(f"\n🔍 Summarizing {os.path.basename(file_path)} ...")

    # ---- Read safely ----
    try:
        df = pd.read_csv(
            file_path,
            low_memory=False,
            on_bad_lines='skip',
            encoding='utf-8'
        )
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)

    print(f"✅ Loaded dataframe with {df.shape[0]:,} rows and {df.shape[1]} columns.")

    # ---- Validate columns ----
    required_cols = [
        "CharacteristicName",
        "MonitoringLocationIdentifier",
        "ActivityStartDate",
        "ResultMeasureValue"
    ]
    for col in required_cols:
        if col not in df.columns:
            print(f"⚠️ Missing expected column: {col}")
            sys.exit(1)

    # ---- Convert datatypes ----
    df["ActivityStartDate"] = pd.to_datetime(df["ActivityStartDate"], errors="coerce")
    df["ResultMeasureValue"] = pd.to_numeric(df["ResultMeasureValue"], errors="coerce")

    # ---- Parameter summary ----
    print("📊 Computing parameter-level statistics ...")
    param_summary = (
        df.groupby("CharacteristicName")
        .agg(
            n_results=("ResultMeasureValue", "count"),
            mean_value=("ResultMeasureValue", "mean"),
            min_value=("ResultMeasureValue", "min"),
            max_value=("ResultMeasureValue", "max"),
            first_date=("ActivityStartDate", "min"),
            last_date=("ActivityStartDate", "max")
        )
        .sort_values("n_results", ascending=False)
    )

    # ---- Site summary ----
    print("📍 Computing site-level statistics ...")
    site_summary = (
        df.groupby("MonitoringLocationIdentifier")
        .agg(
            n_results=("ResultMeasureValue", "count"),
            first_date=("ActivityStartDate", "min"),
            last_date=("ActivityStartDate", "max")
        )
        .sort_values("n_results", ascending=False)
    )

    # ---- Save outputs ----
    base = os.path.splitext(file_path)[0]
    param_out = f"{base}_param_summary.csv"
    site_out = f"{base}_site_summary.csv"

    param_summary.to_csv(param_out)
    site_summary.to_csv(site_out)

    print(f"\n✅ Parameter summary → {param_out}")
    print(f"✅ Site summary → {site_out}")

    # ---- Quick console summary ----
    print("\n📈 Top 10 Parameters by Record Count:")
    print(param_summary.head(10).to_string())

    print("\n📍 Top 10 Sites by Record Count:")
    print(site_summary.head(10).to_string())

    print("\n✨ Done. Skipped malformed lines automatically if present.")
    return param_summary, site_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate summary stats for WQX AMD datasets (fault-tolerant).")
    parser.add_argument("--input", required=True, help="Path to cleaned or AMD-subset CSV file")
    args = parser.parse_args()

    summarize_wqx(args.input)