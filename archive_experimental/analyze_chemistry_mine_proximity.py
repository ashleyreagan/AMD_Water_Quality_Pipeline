#!/usr/bin/env python3
"""
analyze_chemistry_mine_proximity.py
-----------------------------------
Robust CSV loader for large WQX chemistry datasets.
Automatically uses PyArrow for speed and falls back to pandas with error repair.
Cleans mixed datatypes and coerces object columns to string before Parquet export.

Developed by Ashley Mitchell, DOI-OSMRE (2025)
"""

import os
import sys
from datetime import datetime
import pandas as pd
from tqdm import tqdm

LOG_PATH = "data_logs/csv_read_errors.log"


def safe_read_csv_to_parquet(path, out_parquet=None, chunksize=50000):
    """
    Safely load a CSV (prefer PyArrow), repair lines, and save Parquet copy.
    Automatically logs malformed lines and coerces mixed columns to string.
    """
    os.makedirs("data_logs", exist_ok=True)
    bad_lines, parsed_chunks = [], []

    # === Try PyArrow first ===
    try:
        import pyarrow.csv as pv
        import pyarrow.parquet as pq

        print("⚡ Using PyArrow for fast read...")
        table = pv.read_csv(path)
        df = table.to_pandas(types_mapper=pd.ArrowDtype)
        out_parquet = out_parquet or path.replace(".csv", ".parquet")
        pq.write_table(table, out_parquet, compression="snappy")
        print(f"✅ Loaded {len(df):,} rows via PyArrow → saved to {out_parquet}")
        return df

    except ModuleNotFoundError:
        print("⚠️ PyArrow not installed — using pandas fallback.")
    except Exception as e:
        print(f"❌ PyArrow failed ({e}), falling back to pandas...")

    # === Pandas fallback ===
    print("🐢 Reading CSV with pandas fallback...")
    total_size = os.path.getsize(path) / 1e6
    with tqdm(total=total_size, unit="MB", desc="📊 Loading CSV") as pbar:
        try:
            for chunk in pd.read_csv(
                path,
                chunksize=chunksize,
                engine="c",           # use faster parser
                on_bad_lines="skip",  # skip malformed rows
            ):
                parsed_chunks.append(chunk)
                pbar.update(chunksize * 0.0001)
        except Exception as e:
            with open(LOG_PATH, "a") as log:
                log.write(f"[{datetime.now()}] ❌ Fatal error reading {path}: {e}\n")
            raise

    # === Combine and clean ===
    df = pd.concat(parsed_chunks, ignore_index=True)

    # Identify object columns and coerce to string
    obj_cols = [c for c in df.columns if df[c].dtype == "object"]
    if obj_cols:
        with open(LOG_PATH, "a") as log:
            log.write(f"\n[{datetime.now()}] 🧹 Coercing {len(obj_cols)} columns to string: {obj_cols}\n")
        for col in obj_cols:
            df[col] = df[col].astype("string")

    out_parquet = out_parquet or path.replace(".csv", ".parquet")

    # === Write Parquet with safe fallback ===
    try:
        df.to_parquet(out_parquet, compression="snappy")
    except Exception as e:
        # In rare cases, coerce everything to string as last resort
        with open(LOG_PATH, "a") as log:
            log.write(f"[{datetime.now()}] ⚠️ Parquet write failed ({e}) — coercing all columns to string.\n")
        df = df.astype("string")
        df.to_parquet(out_parquet, compression="snappy")

    # === Log summary ===
    with open(LOG_PATH, "a") as log:
        log.write(f"\n[{datetime.now()}] ✅ Loaded {len(df):,} rows from {path}\n")
        if bad_lines:
            log.write(f"⚠️ Skipped {len(bad_lines):,} malformed lines\n")

    print(f"✅ Loaded {len(df):,} rows (saved to {out_parquet})")
    return df


# === MAIN ===
if __name__ == "__main__":
    CHEM_PATH = "data_raw/chemistry/wqx_pa_sites_merged_clean.csv"
    PARQUET_PATH = CHEM_PATH.replace(".csv", ".parquet")

    print("📂 Loading proximity and chemistry data...")

    if os.path.exists(PARQUET_PATH):
        print(f"📦 Found existing Parquet file → {PARQUET_PATH}")
        df = pd.read_parquet(PARQUET_PATH)
    elif os.path.exists(CHEM_PATH):
        print("📄 Reading CSV and creating Parquet version...")
        df = safe_read_csv_to_parquet(CHEM_PATH)
    else:
        print("❌ No CSV or Parquet file found. Please verify file path.")
        sys.exit(1)

    print(f"✅ Dataset ready with {len(df):,} records and {len(df.columns):,} columns.")