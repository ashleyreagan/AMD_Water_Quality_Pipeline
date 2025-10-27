#!/usr/bin/env python3
# ============================================================
# Step 16 — Watershed Health & Treatment Priority Analysis (HUC12)
# ============================================================

import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
import folium
from branca.colormap import LinearColormap

print("🚀 Starting watershed_health_huc12.py")

# ------------------------------------------------------------
# 1️⃣ Load unified dataset
# ------------------------------------------------------------
src = Path("data_outputs/PA_wq_joined_mine_hydro.parquet")
if not src.exists():
    raise FileNotFoundError(f"❌ Missing joined dataset: {src}")

df = pd.read_parquet(src)
print(f"✅ Loaded {len(df):,} records × {len(df.columns)} columns")

# ------------------------------------------------------------
# 2️⃣ Identify relevant fields
# ------------------------------------------------------------
chem_cols = [c for c in ["Fe", "ResultMeasureValue", "Temperatur"] if c in df.columns]
dist_col = "Bituminous_dist_m"
huc_col = "HUC12_ID" if "HUC12_ID" in df.columns else "HUC12"

missing_cols = [c for c in chem_cols + [dist_col, huc_col] if c not in df.columns]
if missing_cols:
    raise KeyError(f"❌ Missing required columns: {missing_cols}")

print(f"🧪 Chemistry columns: {chem_cols}")
print(f"📏 Distance column: {dist_col}")
print(f"💧 HUC identifier: {huc_col}")

# ------------------------------------------------------------
# 3️⃣ Prepare data
# ------------------------------------------------------------
data = df[[huc_col, dist_col] + chem_cols].copy()

# Convert to numeric and drop all-null columns
for c in chem_cols + [dist_col]:
    data[c] = pd.to_numeric(data[c], errors="coerce")
data = data.dropna(subset=[huc_col])

# Drop rows where all chem and dist are NaN
data = data.dropna(subset=[dist_col] + chem_cols, how="all")

# ------------------------------------------------------------
# 4️⃣ Normalize chemistry and distance safely
# ------------------------------------------------------------
def safe_norm(series):
    valid = series.dropna()
    if valid.empty:
        return pd.Series([np.nan] * len(series), index=series.index)
    return (series - valid.min()) / (valid.max() - valid.min())

for c in chem_cols:
    data[c + "_norm"] = safe_norm(data[c])

if data[dist_col].notna().any():
    data["dist_norm"] = safe_norm(data[dist_col])
    data["dist_inv"] = 1 - data["dist_norm"]
else:
    data["dist_inv"] = np.nan

# ------------------------------------------------------------
# 5️⃣ Compute Sickness Index (weighted)
# ------------------------------------------------------------
data["chem_score"] = data[[c + "_norm" for c in chem_cols]].mean(axis=1, skipna=True)
data["Sickness_Index"] = (0.6 * data["chem_score"]) + (0.4 * data["dist_inv"])

# ------------------------------------------------------------
# 6️⃣ Aggregate by HUC12
# ------------------------------------------------------------
summary = (
    data.groupby(huc_col)
    .agg({
        "Fe": "mean",
        "ResultMeasureValue": "mean",
        "Temperatur": "mean",
        dist_col: "mean",
        "Sickness_Index": "mean"
    })
    .reset_index()
)

summary.rename(columns={
    "Fe": "Mean_Fe",
    "ResultMeasureValue": "Mean_Analyte",
    "Temperatur": "Mean_Temp",
    dist_col: "Mean_Dist_M",
}, inplace=True)

summary["Rank"] = summary["Sickness_Index"].rank(ascending=False, method="dense").astype(int)
summary = summary.sort_values("Rank")

print(f"✅ Computed watershed metrics for {len(summary):,} HUC12s")

# ------------------------------------------------------------
# 7️⃣ Merge with HUC12 boundaries
# ------------------------------------------------------------
huc_path = Path("data_cache/HUC12_PA.geojson")
if not huc_path.exists():
    raise FileNotFoundError("❌ Missing HUC12_PA.geojson boundary file.")
huc = gpd.read_file(huc_path)[["huc12", "name", "geometry"]]
huc.rename(columns={"huc12": huc_col, "name": "HUC12_NAME"}, inplace=True)

merged = huc.merge(summary, on=huc_col, how="left")

# Reproject to projected CRS for centroid operations
merged = merged.to_crs(epsg=26917)

# ------------------------------------------------------------
# 8️⃣ Export datasets
# ------------------------------------------------------------
out_dir = Path("data_outputs")
out_dir.mkdir(exist_ok=True)

merged.to_file(out_dir / "PA_wq_HUC12_health_map.geojson", driver="GeoJSON")
summary.to_csv(out_dir / "PA_wq_watershed_health_HUC12.csv", index=False)
summary.to_parquet(out_dir / "PA_wq_watershed_health_HUC12.parquet")

top10 = merged.nlargest(10, "Sickness_Index")[[
    huc_col, "HUC12_NAME", "Mean_Fe", "Mean_Analyte", "Mean_Temp", "Mean_Dist_M", "Sickness_Index"
]]
top10.to_csv(out_dir / "PA_treatment_priority_top10.csv", index=False)
print("💾 Exported CSVs and GeoJSON successfully.")

# ------------------------------------------------------------
# 9️⃣ Write methodology text
# ------------------------------------------------------------
methodology_text = f"""
Passive Treatment Priority Ranking — Pennsylvania HUC12 Watersheds
==================================================================

Data Sources:
- EPA WQX chemistry (Fe, general analytes, temperature)
- PASDA Bituminous Mine Inventory (distance: {dist_col})
- PASDA HUC12 boundaries (2023)
- Unified dataset: {src.name}

Computation:
-------------
1. Chemistry and distance normalized safely (ignoring NaNs).
2. Distance inverted so proximity increases index severity.
3. Composite index:
      S = 0.6 × Chemistry + 0.4 × (1 − Normalized Distance)
4. Aggregated to HUC12, ranked by descending S.
5. Output exports: .csv, .geojson, .html, .txt.

Interpretation:
---------------
High S = higher concern. Use Top 10 file for field validation.
"""
with open(out_dir / "PA_treatment_priority_methodology.txt", "w") as f:
    f.write(methodology_text)

print("🧾 Methodology file written.")

# ------------------------------------------------------------
# 🔟 Folium Map Export
# ------------------------------------------------------------
mapped = merged.dropna(subset=["Sickness_Index"]).to_crs(epsg=4326)

if not mapped["Sickness_Index"].notna().any():
    print("⚠️ No valid Sickness_Index values for mapping. Skipping map export.")
else:
    vmin = float(mapped["Sickness_Index"].min(skipna=True))
    vmax = float(mapped["Sickness_Index"].max(skipna=True))
    if np.isnan(vmin) or np.isnan(vmax):
        print("⚠️ Invalid color scale bounds — skipping color map.")
    else:
        colormap = LinearColormap(
            colors=["green", "yellow", "orange", "red"],
            vmin=vmin, vmax=vmax,
            caption="Sickness Index (0–1)"
        )

        # Compute map center from valid centroids
        merged_valid = mapped[mapped.geometry.notna()]
        center = [
            merged_valid.geometry.centroid.y.mean(),
            merged_valid.geometry.centroid.x.mean()
        ]

        m = folium.Map(location=center, zoom_start=7, tiles="CartoDB positron")

        def style_fn(feature):
            val = feature["properties"].get("Sickness_Index", None)
            color = colormap(val) if val is not None else "#cccccc"
            return {"fillColor": color, "color": "#555555", "weight": 0.3, "fillOpacity": 0.7}

        folium.GeoJson(mapped, style_function=style_fn,
                       tooltip=folium.GeoJsonTooltip(
                           fields=[huc_col, "HUC12_NAME", "Sickness_Index"],
                           aliases=["HUC12", "Watershed", "Sickness Index"]
                       )).add_to(m)

        colormap.add_to(m)
        out_map = out_dir / "PA_wq_HUC12_health_map.html"
        m.save(out_map)
        print(f"🗺️ Saved interactive map → {out_map}")

print("🎯 Watershed health analysis complete.")