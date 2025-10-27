# ============================================================
# AMD Early Warning – WQX/NWIS Data Download and Cleaning
# ============================================================
# Author: Ashley R. Mitchell, OSMRE (2025)
# Purpose: Retrieve Pennsylvania mine drainage and surface water
#          chemistry from WaterQualityData.us and prepare for ML pipeline
# ============================================================

import pandas as pd
import requests, zipfile, io, os
from tqdm import tqdm

# -----------------------------
# 1️⃣ Output path setup
# -----------------------------
os.makedirs("data_raw/chemistry", exist_ok=True)
output_file = "data_raw/chemistry/wq_samples.csv"

# -----------------------------
# 2️⃣ WQX query parameters (use list-of-tuples format!)
# -----------------------------
params = [
    ("statecode", "US:42"),
    ("providers", "NWIS"),
    ("providers", "STORET"),
    ("siteType", "Mine Drainage"),
    ("siteType", "Stream"),
    ("siteType", "Surface Water"),
    ("siteType", "Outfall"),
    ("CharacteristicName", "pH"),
    ("CharacteristicName", "Iron"),
    ("CharacteristicName", "Manganese"),
    ("CharacteristicName", "Sulfate"),
    ("CharacteristicName", "Alkalinity"),
    ("CharacteristicName", "Specific conductance"),
    ("mimeType", "csv"),
    ("zip", "yes"),
]

url = "https://www.waterqualitydata.us/data/Result/search"

# -----------------------------
# 3️⃣ Download zipped CSV
# -----------------------------
print("📡 Downloading WQX data for Pennsylvania...")
r = requests.get(url, params=params, stream=True, timeout=300)

if r.status_code != 200 or len(r.content) < 1000:
    raise RuntimeError(f"Download failed or returned empty document. HTTP {r.status_code}")

z = zipfile.ZipFile(io.BytesIO(r.content))
csv_name = z.namelist()[0]
print(f"✅ Download complete: {csv_name}")

# -----------------------------
# 4️⃣ Read into DataFrame
# -----------------------------
print("📥 Reading CSV into pandas...")
df = pd.read_csv(z.open(csv_name), low_memory=False)

# -----------------------------
# 5️⃣ Keep only relevant columns
# -----------------------------
keep_cols = [
    "OrganizationIdentifier",
    "MonitoringLocationIdentifier",
    "MonitoringLocationName",
    "MonitoringLocationTypeName",
    "ActivityStartDate",
    "LatitudeMeasure",
    "LongitudeMeasure",
    "CharacteristicName",
    "ResultMeasureValue",
    "ResultMeasure.MeasureUnitCode"
]
df = df[keep_cols]
df = df.dropna(subset=["LatitudeMeasure", "LongitudeMeasure", "ResultMeasureValue"])
print(f"✅ Retained {len(df):,} valid chemistry records.")

# -----------------------------
# 6️⃣ Clean characteristic names
# -----------------------------
rename_map = {
    "Specific conductance": "Conductivity",
    "Iron": "Fe_mgL",
    "Manganese": "Mn_mgL",
    "Sulfate": "SO4_mgL",
    "Alkalinity": "Alk_mgL",
    "pH": "pH"
}
df["CharacteristicName"] = df["CharacteristicName"].replace(rename_map)

# -----------------------------
# 7️⃣ Basic unit normalization
# -----------------------------
# Convert µS/cm to mS/cm if necessary
df.loc[
    (df["CharacteristicName"] == "Conductivity") &
    (df["ResultMeasure.MeasureUnitCode"].isin(["uS/cm","µS/cm"])),
    "ResultMeasureValue"
] = df["ResultMeasureValue"] / 1000

df["ResultMeasure.MeasureUnitCode"] = df["ResultMeasure.MeasureUnitCode"].str.replace("mg/l", "mg/L", case=False)

# -----------------------------
# 8️⃣ Pivot to wide format (per site)
# -----------------------------
print("📊 Pivoting to wide format...")
df_wide = (
    df.pivot_table(
        index=["MonitoringLocationIdentifier","LatitudeMeasure","LongitudeMeasure","MonitoringLocationTypeName"],
        columns="CharacteristicName",
        values="ResultMeasureValue",
        aggfunc="mean"
    )
    .reset_index()
)
df_wide.columns.name = None
df_wide.rename(columns={
    "LatitudeMeasure": "latitude",
    "LongitudeMeasure": "longitude",
    "MonitoringLocationTypeName": "site_type"
}, inplace=True)

# -----------------------------
# 9️⃣ Derive AMD indicator
# -----------------------------
df_wide["AMD_present"] = (
    (df_wide["pH"] < 6.0) |
    (df_wide["Fe_mgL"] > 1.0) |
    (df_wide["SO4_mgL"] > 250)
).astype(int)

# -----------------------------
# 🔟 Save cleaned dataset
# -----------------------------
df_wide.to_csv(output_file, index=False)
print(f"✅ Saved cleaned chemistry data to: {output_file}")
print(f"💾 Rows: {len(df_wide):,}, Columns: {len(df_wide.columns)}")
print(df_wide.head())