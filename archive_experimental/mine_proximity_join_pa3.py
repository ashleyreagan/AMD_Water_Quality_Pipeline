#!/usr/bin/env python3
"""
mine_proximity_join_pa3.py
Step 4 of Pennsylvania AMD workflow.
Joins WQX chemistry sites with nearest mine/AML features.
"""

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

# -----------------------------------------------------------
# Paths
# -----------------------------------------------------------
CHEM_PARQUET = "data_raw/chemistry/wqx_pa_sites_merged_clean.parquet"
DATA_CACHE = "data_cache"

# Use the AMD-focused subset of AML inventory (preferred)
AML_PATH = os.path.join(DATA_CACHE, "aml_inventory_AMDsubset.geojson")

BIT_PATH = os.path.join(DATA_CACHE, "bituminous_surface.geojson")
ANT_PATH = os.path.join(DATA_CACHE, "anthracite_surface.geojson")
OUT_PATH = "data_outputs/pa_wqx_mine_join.parquet"

# -----------------------------------------------------------
# Helper: safely load a GeoJSON layer
# -----------------------------------------------------------
def safe_load_geojson(path, name):
    if not os.path.exists(path):
        print(f"⚠️ Warning: {name} file not found at {path}. Skipping this layer.")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
    try:
        gdf = gpd.read_file(path)
        print(f"✅ Loaded {name} ({len(gdf)} features)")
        return gdf
    except Exception as e:
        print(f"❌ Failed to load {name}: {e}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

# -----------------------------------------------------------
# Load chemistry data (no geo metadata yet)
# -----------------------------------------------------------
print("📂 Loading cleaned chemistry dataset…")
chem_df = pd.read_parquet(CHEM_PARQUET)

if not {"LatitudeMeasure", "LongitudeMeasure"}.issubset(chem_df.columns):
    raise ValueError("Missing latitude/longitude columns in chemistry dataset")

chem = gpd.GeoDataFrame(
    chem_df,
    geometry=gpd.points_from_xy(chem_df["LongitudeMeasure"], chem_df["LatitudeMeasure"]),
    crs="EPSG:4326"
)

# -----------------------------------------------------------
# Load mine feature layers (with fallback)
# -----------------------------------------------------------
print("📂 Loading mine feature layers…")
aml = safe_load_geojson(AML_PATH, "AML Inventory")
bit = safe_load_geojson(BIT_PATH, "Bituminous Surface Mines")
ant = safe_load_geojson(ANT_PATH, "Anthracite Surface Mines")

# Skip if all empty
if all(gdf.empty for gdf in [aml, bit, ant]):
    raise SystemExit("❌ No mine datasets available — please check your data_cache folder.")

# Reproject to NAD83 / UTM Zone 17N
target_crs = "EPSG:26917"
for gdf in [chem, aml, bit, ant]:
    if not gdf.empty:
        gdf.to_crs(target_crs, inplace=True)

# -----------------------------------------------------------
# Compute nearest mine feature for each site
# -----------------------------------------------------------
def nearest_feature(row, target_gdf):
    """Find nearest geometry and distance (m)."""
    if row.geometry is None or target_gdf.empty:
        return None, None
    nearest_geom = target_gdf.geometry.iloc[target_gdf.geometry.distance(row.geometry).idxmin()]
    distance = row.geometry.distance(nearest_geom)
    return nearest_geom, distance

layers = {"AML": aml, "Bituminous": bit, "Anthracite": ant}

print("📏 Computing nearest mine features…")
for name, layer in layers.items():
    tqdm.pandas(desc=f"→ {name}")
    chem[[f"{name}_geom", f"{name}_dist_m"]] = chem.progress_apply(
        lambda r: nearest_feature(r, layer), axis=1, result_type="expand"
    )

# -----------------------------------------------------------
# Select the nearest overall mine feature
# -----------------------------------------------------------
dist_cols = [f"{n}_dist_m" for n in layers]
chem["nearest_type"] = chem[dist_cols].idxmin(axis=1).str.replace("_dist_m", "")
chem["nearest_dist_m"] = chem[dist_cols].min(axis=1)

# -----------------------------------------------------------
# Save output as GeoParquet
# -----------------------------------------------------------
os.makedirs("data_outputs", exist_ok=True)
chem.to_parquet(OUT_PATH, compression="snappy")
print(f"✅ Saved joined dataset to {OUT_PATH}")
print(f"✅ {len(chem):,} records processed.")