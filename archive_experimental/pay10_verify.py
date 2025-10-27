#!/usr/bin/env python3
# ===============================================================
# Pennsylvania AMD Step 10B — Verification & Summary
# ===============================================================

import geopandas as gpd
import pandas as pd
from pathlib import Path

print("🔍 Starting pa10_verify.py")

out_path = Path("data_outputs/PA_wq_joined_mine_hydro.parquet")
if not out_path.exists():
    raise SystemExit("❌ Unified parquet not found. Run Step 10 first.")

df = gpd.read_parquet(out_path)
print(f"✅ Loaded unified dataset: {len(df):,} records")

# Check missing geometries
missing_geom = df["geometry"].isna().sum()
print(f"🧩 Missing geometries: {missing_geom:,}")

# Summaries by HUC10 and HUC12
def summarize(field):
    if field not in df.columns:
        print(f"⚠️ {field} not present.")
        return None
    summary = df.groupby(field).size().reset_index(name="count").sort_values("count", ascending=False)
    print(f"\n📊 Top {field} entries:")
    print(summary.head(10))
    return summary

summarize("HUC10_NAME")
summarize("HUC12_NAME")

# Hydrology sanity checks
hydro_cols = [c for c in df.columns if "FLOW" in c.upper() or "HUC" in c.upper()]
print("\n💧 Hydrology-related columns detected:")
print(hydro_cols)

print("🎯 Verification complete.")ß