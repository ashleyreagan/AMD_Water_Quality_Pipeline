# ==========================================================
# chemistry_standardize.py
# Clean and standardize AMD chemistry data for modeling
# ==========================================================

import pandas as pd
import numpy as np
from pathlib import Path

# Paths
data_path = Path("data_outputs/PA_wq_joined_mine_hydro.parquet")
out_clean = Path("data_outputs/PA_wq_chemistry_clean.parquet")
out_summary = Path("data_outputs/PA_wq_chemistry_summary.csv")

print(f"🔍 Loading {data_path.name} ...")
df = pd.read_parquet(data_path)

# Detect chemistry columns
chem_cols = ["Fe", "pH_in", "pH_out", "CondIn", "CondOut", "TempIn", "TempOut", "Temperatur"]
chem_present = [c for c in chem_cols if c in df.columns]
print(f"✅ Found {len(chem_present)} chemistry columns: {', '.join(chem_present)}")

# Keep only rows with at least one non-null chemistry value
mask = df[chem_present].notna().any(axis=1)
chem_df = df.loc[mask].copy()
print(f"🧪 Retained {len(chem_df):,} rows with chemistry data ({len(chem_df)/len(df)*100:.1f}%).")

# Normalize column names
rename_map = {
    "pH_in": "pH_In",
    "pH_out": "pH_Out",
    "CondIn": "Cond_In",
    "CondOut": "Cond_Out",
    "TempIn": "Temp_In",
    "TempOut": "Temp_Out",
    "Temperatur": "Temp"
}
chem_df.rename(columns=rename_map, inplace=True)

# Compute derived deltas
for pair, delta in [
    (("pH_In", "pH_Out"), "ΔpH"),
    (("Cond_In", "Cond_Out"), "ΔCond"),
    (("Temp_In", "Temp_Out"), "ΔTemp"),
]:
    a, b = pair
    if a in chem_df.columns and b in chem_df.columns:
        chem_df[delta] = chem_df[b] - chem_df[a]

# Basic stats summary
summary = chem_df[["Fe", "pH_In", "pH_Out", "Cond_In", "Cond_Out", "Temp_In", "Temp_Out"]].describe(include="all")
summary.to_csv(out_summary)
print(f"💾 Summary saved → {out_summary}")

# Save cleaned subset
chem_df.to_parquet(out_clean, index=False)
print(f"💾 Clean chemistry data saved → {out_clean}")
print("✅ Done.")