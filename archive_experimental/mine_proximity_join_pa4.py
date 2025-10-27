#!/usr/bin/env python3
"""
mine_proximity_join_pa4.py
Optimized proximity join for Pennsylvania AMD project.
Joins WQX chemistry sites to nearest mine/AML features using spatial indexes.
"""

import os
import time
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

# -----------------------------------------------------------
# Paths
# -----------------------------------------------------------
CHEM_PARQUET = "data_raw/chemistry/wqx_pa_sites_merged_clean.parquet"
DATA_CACHE = "data_cache"
AML_PATH = os.path.join(DATA_CACHE, "aml_inventory_AMDsubset.geojson")
BIT_PATH = os.path.join(DATA_CACHE, "bituminous_surface.geojson")
ANT_PATH = os.path.join(DATA_CACHE, "anthracite_surface.geojson")

OUT_PARQUET = "data_outputs/pa_wqx_mine_join.parquet"
OUT_PREVIEW = "data_outputs/pa_wqx_mine_join_preview.geojson"
LOG_PATH = "logs/missing_geometry_qc.txt"

os.makedirs("logs", exist_ok=True)
os.makedirs("data_outputs", exist_ok=True)

# -----------------------------------------------------------
# Helper: safe GeoJSON loader with fallback
# -----------------------------------------------------------
def safe_load_geojson(path, name):
    if not os.path.exists(path):
        print(f"⚠️  {name} not found at {path}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
    try:
        gdf = gpd.read_file(path)
        print(f"✅ Loaded {name}: {len(gdf)} features")
        return gdf
    except Exception as e:
        print(f"❌ Failed to load {name}: {e}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

# -----------------------------------------------------------
# Step 1: Load chemistry sites
# -----------------------------------------------------------
print("📂 Loading chemistry dataset…")
chem_df = pd.read_parquet(CHEM_PARQUET)

if not {"LatitudeMeasure", "LongitudeMeasure"}.issubset(chem_df.columns):
    raise ValueError("Missing latitude/longitude columns in chemistry dataset")

chem = gpd.GeoDataFrame(
    chem_df,
    geometry=gpd.points_from_xy(chem_df["LongitudeMeasure"], chem_df["LatitudeMeasure"]),
    crs="EPSG:4326"
)
print(f"✅ Chemistry records: {len(chem)}")

# -----------------------------------------------------------
# Step 2: Load mine/AML layers
# -----------------------------------------------------------
print("📂 Loading mine feature layers…")
aml = safe_load_geojson(AML_PATH, "AML Inventory (AMD subset)")
bit = safe_load_geojson(BIT_PATH, "Bituminous Surface Mines")
ant = safe_load_geojson(ANT_PATH, "Anthracite Surface Mines")

layers = {"AML": aml, "Bituminous": bit, "Anthracite": ant}

# -----------------------------------------------------------
# Step 3: Clean geometries + log invalids
# -----------------------------------------------------------
log_lines = []
for name, gdf in layers.items():
    invalid_mask = ~gdf.geometry.is_valid | gdf.geometry.is_empty | gdf.geometry.isna()
    n_invalid = invalid_mask.sum()
    if n_invalid > 0:
        log_lines.append(f"{name}: Dropped {n_invalid} invalid geometries\n")
        gdf.drop(index=gdf[invalid_mask].index, inplace=True)
    layers[name] = gdf

invalid_chem = ~chem.geometry.is_valid | chem.geometry.is_empty | chem.geometry.isna()
if invalid_chem.sum() > 0:
    log_lines.append(f"Chemistry: Dropped {invalid_chem.sum()} invalid geometries\n")
    chem = chem.loc[~invalid_chem].copy()

if log_lines:
    with open(LOG_PATH, "w") as f:
        f.writelines(log_lines)
    print(f"🧹 Logged invalid geometry cleanup to {LOG_PATH}")

# -----------------------------------------------------------
# Step 4: Reproject all to UTM Zone 17N
# -----------------------------------------------------------
target_crs = "EPSG:26917"
chem = chem.to_crs(target_crs)
for k, gdf in layers.items():
    if not gdf.empty:
        layers[k] = gdf.to_crs(target_crs)

# -----------------------------------------------------------
# Step 5: Spatial index nearest neighbor join
# -----------------------------------------------------------
def nearest_join(source_gdf, target_gdf, target_name):
    """Find nearest geometry and distance efficiently using spatial index."""
    if target_gdf.empty:
        return pd.Series([None, None])

    sindex = target_gdf.sindex
    distances = []
    idxs = []

    for geom in tqdm(source_gdf.geometry, desc=f"→ {target_name}", ncols=80):
        if geom is None or geom.is_empty:
            distances.append(None)
            idxs.append(None)
            continue
        # use spatial index
        possible_matches_index = list(sindex.nearest(geom.bounds, 1))
        nearest_geom = target_gdf.iloc[possible_matches_index[0]].geometry
        dist = geom.distance(nearest_geom)
        idxs.append(possible_matches_index[0])
        distances.append(dist)

    return pd.Series([idxs, distances])

print("📏 Running optimized nearest-neighbor join…")
for name, layer in layers.items():
    chem[[f"{name}_idx", f"{name}_dist_m"]] = nearest_join(chem, layer, name)

# -----------------------------------------------------------
# Step 6: Determine nearest overall
# -----------------------------------------------------------
dist_cols = [f"{n}_dist_m" for n in layers]
chem["nearest_type"] = chem[dist_cols].idxmin(axis=1).str.replace("_dist_m", "")
chem["nearest_dist_m"] = chem[dist_cols].min(axis=1)

# -----------------------------------------------------------
# Step 7: Export results
# -----------------------------------------------------------
chem.to_parquet(OUT_PARQUET, compression="snappy")
print(f"✅ Saved {len(chem):,} records to {OUT_PARQUET}")

# Optional GeoJSON preview for QGIS
if len(chem) > 10000:
    chem.iloc[:10000].to_file(OUT_PREVIEW, driver="GeoJSON")
    print(f"🗺️  Wrote 10k record preview to {OUT_PREVIEW}")
else:
    chem.to_file(OUT_PREVIEW, driver="GeoJSON")
    print(f"🗺️  Wrote full GeoJSON to {OUT_PREVIEW}")

print("🏁 Done.")