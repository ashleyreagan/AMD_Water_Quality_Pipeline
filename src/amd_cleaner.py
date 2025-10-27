#!/usr/bin/env python3
"""
AMD Cleaner – v1.0
Author: Ashley Mitchell, OSMRE 2025

Cleans and standardizes WQX chemistry data for AMD analysis.
Handles multi-million row CSVs safely using chunked loading.
"""

import pandas as pd
import argparse
import os

def clean_wqx_file(input_path, state_code):
    print(f"🧹 Cleaning file: {input_path}")

    # Output paths
    out_clean = f"wqx_{state_code.lower()}_sites_merged_clean.csv"
    out_subset = f"wqx_{state_code.lower()}_sites_merged_AMDsubset.csv"

    # Define AMD parameters of interest
    amd_params = [
        "pH", "Iron", "Manganese", "Aluminum",
        "Sulfate", "Alkalinity", "Specific conductance"
    ]

    # Create writers for large file streaming
    chunk_size = 250000
    first_clean = True
    first_subset = True

    for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunk_size, low_memory=False)):
        print(f"📦 Processing chunk {i+1} ...")

        # Drop empty or duplicate columns
        chunk = chunk.loc[:, chunk.notna().any()]
        chunk = chunk.loc[:, ~chunk.columns.duplicated()]

        # Clean headers
        chunk.columns = (
            chunk.columns.str.strip()
            .str.replace(" ", "_")
            .str.replace("/", "_")
            .str.replace("-", "_")
        )

        # Trim whitespace from text fields
        text_cols = chunk.select_dtypes(include="object").columns
        chunk[text_cols] = chunk[text_cols].apply(lambda x: x.str.strip())

        # Coerce numeric fields
        num_fields = [c for c in chunk.columns if "MeasureValue" in c or "Result" in c or "Detection" in c]
        for col in num_fields:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        # Drop duplicates within chunk
        chunk = chunk.drop_duplicates()

        # Write cleaned chunk
        mode = "w" if first_clean else "a"
        header = first_clean
        chunk.to_csv(out_clean, mode=mode, header=header, index=False)
        first_clean = False

        # Filter for AMD subset and write
        if "CharacteristicName" in chunk.columns:
            subset = chunk[chunk["CharacteristicName"].isin(amd_params)]
            if not subset.empty:
                mode = "w" if first_subset else "a"
                header = first_subset
                subset.to_csv(out_subset, mode=mode, header=header, index=False)
                first_subset = False

    print(f"✅ Cleaned file saved to: {out_clean}")
    print(f"✅ AMD subset saved to: {out_subset}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and subset WQX chemistry data for AMD analysis.")
    parser.add_argument("--state", required=True, help="State abbreviation (e.g., PA, WV, KY)")
    parser.add_argument("--input", required=True, help="Path to merged WQX CSV file")
    args = parser.parse_args()

    clean_wqx_file(args.input, args.state)