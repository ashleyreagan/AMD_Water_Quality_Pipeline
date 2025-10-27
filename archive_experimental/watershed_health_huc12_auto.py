#!/usr/bin/env python3
# ============================================================
# Watershed Health – HUC12 (Clean Merge, Robust v2)
# ============================================================

import pandas as pd
import geopandas as gpd
import numpy as np
import folium
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler

print("🚀 Starting watershed_health_huc12_cleanmerge_v2.py")

# ------------------------- 1) INPUTS -------------------------
joined_path = Path("data_outputs/PA_wq_joined_mine_hydro.parquet")
huc_path    = Path("data_cache/HUC12_PA.geojson")
out_dir     = Path("data_outputs"); out_dir.mkdir(exist_ok=True)

if not joined_path.exists():
    raise FileNotFoundError(f"❌ Joined dataset not found: {joined_path}")
if not huc_path.exists():
    raise FileNotFoundError(f"❌ HUC12 GeoJSON not found: {huc_path}")

# Use pandas (no geometry needed for stats)
df = pd.read_parquet(joined_path)
print(f"✅ Loaded joined dataset: {len(df):,} rows × {len(df.columns)} cols")

# ------------------ 2) MERGE IN/OUT ANALYTES -----------------
def merge_mean(df, base):
    ins  = [c for c in df.columns if c.lower().endswith("in")  and base.lower() in c.lower()]
    outs = [c for c in df.columns if c.lower().endswith("out") and base.lower() in c.lower()]
    cols = ins + outs
    if not cols:
        return None
    merged = df[cols].replace(0, np.nan).apply(pd.to_numeric, errors="coerce").mean(axis=1)
    name = f"{base}_mean"
    df[name] = merged
    return name

merged_fields = []
for base in ["Iron", "Cond", "Temp"]:
    nm = merge_mean(df, base)
    if nm: merged_fields.append(nm)

if not merged_fields:
    raise SystemExit("❌ No In/Out analytes found to merge (Iron/Cond/Temp).")

print(f"🧪 Merged analytes: {merged_fields}")

# ------------------- 3) DISTANCE DETECTION -------------------
dist_candidates = [c for c in df.columns if "dist" in c.lower()]
# prefer AML_dist_m, then Bituminous_dist_m, else first found
pref_order = ["AML_dist_m", "Bituminous_dist_m"]
dist_col = next((c for c in pref_order if c in df.columns), None)
if dist_col is None:
    dist_col = dist_candidates[0] if dist_candidates else None

if dist_col is None:
    print("⚠️ No distance field found; proximity weighting disabled.")
else:
    print(f"📏 Using distance field: {dist_col}")

# -------------------- 4) HUC ID DETECTION --------------------
# Prefer a clear HUC12 ID from the table
huc_id = next((c for c in df.columns if c.lower() == "huc12_id"), None)
if huc_id is None:
    # fallback: any column containing huc12
    huc_candidates = [c for c in df.columns if "huc12" in c.lower()]
    if not huc_candidates:
        raise KeyError("❌ No HUC12 ID column found in joined dataset.")
    huc_id = huc_candidates[0]
print(f"💧 Grouping by HUC column in table: {huc_id}")

# ------------------ 5) SICKNESS INDEX (per row) ---------------
scaler = MinMaxScaler()

def safe_norm(series):
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if s.dropna().empty:
        return np.zeros(len(s))
    # fill NaNs with median to stabilize scaling
    s_f = s.fillna(s.median())
    return scaler.fit_transform(s_f.to_numpy().reshape(-1,1)).flatten()

# equal weights here (you can tweak)
weights = {"Iron_mean": 0.4, "Cond_mean": 0.4, "Temp_mean": 0.2}

df["Sickness_Index"] = 0.0
for col, w in weights.items():
    if col in df.columns:
        df["Sickness_Index"] += w * safe_norm(df[col])

# proximity multiplier (closer = worse)
if dist_col is not None and df[dist_col].notna().any():
    prox = 1 - safe_norm(df[dist_col])
    df["Sickness_Index"] *= (0.5 + 0.5 * prox)
else:
    print("ℹ️ Proximity multiplier skipped (no valid distance).")

# ------------------ 6) HUC12 AGGREGATION ---------------------
agg_dict = {k: "mean" for k in merged_fields}
agg_dict.update({"Sickness_Index": "mean"})
if dist_col: agg_dict.update({dist_col: "mean"})

summary = df.groupby(huc_id).agg(agg_dict).reset_index()
summary["Rank"] = summary["Sickness_Index"].rank(ascending=False, method="dense").astype(int)
summary = summary.sort_values("Rank")
print(f"✅ Summarized {len(summary):,} HUC12s")

# ------------------ 7) JOIN POLYGONS SAFELY ------------------
huc = gpd.read_file(huc_path)
huc_key = next((c for c in huc.columns if c.lower() == "huc12"), None)
if huc_key is None:
    huc_key = next((c for c in huc.columns if "huc12" in c.lower()), None)
if huc_key is None:
    raise KeyError("❌ No HUC12 key found in GeoJSON.")

# keep a friendlier name if present
name_field = next((c for c in huc.columns if c.lower() == "name"), None)
huc = huc[[huc_key, "geometry"] + ([name_field] if name_field else [])].copy()

merged_gdf = huc.merge(summary, left_on=huc_key, right_on=huc_id, how="left")

# For Folium, use WGS84
merged_gdf = merged_gdf.to_crs(epsg=4326)
print(f"✅ Polygons merged: {len(merged_gdf)} (key: shapefile='{huc_key}' ↔ table='{huc_id}')")

# ---------------------- 8) EXPORTS ---------------------------
csv_path     = out_dir / "PA_wq_watershed_health_HUC12_cleaned.csv"
geojson_path = out_dir / "PA_wq_HUC12_health_map_cleaned.geojson"
top10_path   = out_dir / "PA_treatment_priority_top10_cleaned.csv"
map_path     = out_dir / "PA_wq_HUC12_health_map_cleaned.html"
txt_path     = out_dir / "PA_treatment_priority_methodology.txt"

summary.to_csv(csv_path, index=False)
merged_gdf.to_file(geojson_path, driver="GeoJSON")
merged_gdf.nlargest(10, "Sickness_Index").to_csv(top10_path, index=False)
print(f"💾 Saved:\n  • {csv_path}\n  • {geojson_path}\n  • {top10_path}")

# ---------------------- 9) MAP (robust) ----------------------
mapped = merged_gdf.dropna(subset=["Sickness_Index"]).copy()
if mapped.empty:
    print("⚠️ No valid Sickness_Index for mapping; skipping map export.")
else:
    vmin = float(mapped["Sickness_Index"].min())
    vmax = float(mapped["Sickness_Index"].max())
    if not np.isfinite(vmin) or not np.isfinite(vmax):
        print("⚠️ Invalid color scale bounds; skipping map export.")
    else:
        # Center from valid centroids
        cy = mapped.geometry.centroid.y.mean()
        cx = mapped.geometry.centroid.x.mean()
        m = folium.Map(location=[cy, cx], zoom_start=7, tiles="CartoDB positron")

        folium.Choropleth(
            geo_data=mapped,     # pass GDF directly
            data=mapped,
            columns=[huc_key, "Sickness_Index"],
            key_on=f"feature.properties.{huc_key}",
            fill_color="YlOrRd",
            legend_name="Sickness Index (HUC12, merged)",
            fill_opacity=0.7, line_opacity=0.2,
        ).add_to(m)

        m.save(map_path)
        print(f"🗺️ Map → {map_path}")

# -------------------- 10) METHODOLOGY TXT --------------------
method = f"""PA HUC12 Watershed Health — Clean Merge v2

Inputs:
- Joined table: {joined_path.name}
- HUC12 polygons: {huc_path.name}

Analytes (merged In/Out):
- Iron_mean, Cond_mean, Temp_mean

Index:
- Per record: 0.4·norm(Iron_mean) + 0.4·norm(Cond_mean) + 0.2·norm(Temp_mean)
- Optional proximity multiplier: × (0.5 + 0.5·(1 − norm(distance)))
- Aggregation: mean by {huc_id}
- Rank: descending Sickness_Index (1 = worst)

Join:
- Geo join key shapefile='{huc_key}' ↔ table='{huc_id}'

Outputs:
- {csv_path.name}
- {geojson_path.name}
- {top10_path.name}
- {map_path.name if map_path.exists() else '(map skipped)'}
"""
with open(txt_path, "w") as f:
    f.write(method)
print(f"📘 Methodology → {txt_path}")

print("🎯 Done.")