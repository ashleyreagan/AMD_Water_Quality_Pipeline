#!/usr/bin/env python3
"""
mine_proximity_join_pa5.py
Step 4 of Pennsylvania AMD workflow.
Optimized nearest-mine join with parallel fallback and QC logging.
"""

import os
import sys
import traceback
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.errors import ShapelyError
from shapely import speedups
from tqdm import tqdm
from joblib import Parallel, delayed
import multiprocessing as mp
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

if speedups.available:
    speedups.enable()

# -----------------------------------------------------------
# Paths
# -----------------------------------------------------------
CHEM_PARQUET = "data_raw/chemistry/wqx_pa_sites_merged_clean.parquet"
DATA_CACHE   = "data_cache"
OUT_PATH     = "data_outputs/pa_wqx_mine_join.parquet"
LOG_DIR      = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

AML_PATH = os.path.join(DATA_CACHE, "aml_inventory_AMDsubset.geojson")
BIT_PATH = os.path.join(DATA_CACHE, "bituminous_surface.geojson")
ANT_PATH = os.path.join(DATA_CACHE, "anthracite_surface.geojson")

# -----------------------------------------------------------
# Helper: safe GeoJSON loader
# -----------------------------------------------------------
def safe_load_geojson(path, name):
    if not os.path.exists(path):
        print(f"⚠️ {name} not found: {path}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
    try:
        gdf = gpd.read_file(path)
        gdf = gdf.dropna(subset=["geometry"])
        gdf = gdf[gdf.is_valid]
        print(f"✅ Loaded {name}: {len(gdf)} features")
        return gdf
    except Exception as e:
        print(f"❌ Failed {name}: {e}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

# -----------------------------------------------------------
# Load chemistry
# -----------------------------------------------------------
print("📂 Loading chemistry dataset…")
chem_df = pd.read_parquet(CHEM_PARQUET)
if not {"LatitudeMeasure","LongitudeMeasure"}.issubset(chem_df.columns):
    raise SystemExit("❌ Missing Latitude/Longitude columns in chemistry dataset")

chem = gpd.GeoDataFrame(
    chem_df,
    geometry=gpd.points_from_xy(chem_df["LongitudeMeasure"], chem_df["LatitudeMeasure"]),
    crs="EPSG:4326"
).dropna(subset=["geometry"])
chem = chem[chem.is_valid]
print(f"✅ Chemistry records: {len(chem):,}")

# -----------------------------------------------------------
# Load mines
# -----------------------------------------------------------
print("📂 Loading mine feature layers…")
aml = safe_load_geojson(AML_PATH, "AML Inventory (AMD subset)")
bit = safe_load_geojson(BIT_PATH, "Bituminous Surface Mines")
ant = safe_load_geojson(ANT_PATH, "Anthracite Surface Mines")

layers = {"AML": aml, "Bituminous": bit, "Anthracite": ant}
if all(g.empty for g in layers.values()):
    raise SystemExit("❌ No mine datasets found")

# Reproject to UTM 17N
target_crs = "EPSG:26917"
for gdf in [chem] + list(layers.values()):
    if not gdf.empty:
        gdf.to_crs(target_crs, inplace=True)

# -----------------------------------------------------------
# Nearest neighbor using spatial index
# -----------------------------------------------------------
def nearest_distance(src_geom, sindex, target_geoms):
    try:
        idx = list(sindex.nearest(src_geom, 1))[0]
        return idx, src_geom.distance(target_geoms.iloc[idx])
    except Exception:
        return None, None

def process_chunk(df_chunk, layer_name, layer_gdf):
    sindex = layer_gdf.sindex
    results = []
    for _, row in df_chunk.iterrows():
        if row.geometry is None or not row.geometry.is_valid:
            results.append((None, None))
            continue
        idx, dist = nearest_distance(row.geometry, sindex, layer_gdf.geometry)
        results.append((idx, dist))
    return results

def nearest_join_parallel(chem, layer_gdf, name):
    """Try parallel nearest; fallback to sequential if error."""
    if layer_gdf.empty:
        chem[[f"{name}_idx", f"{name}_dist_m"]] = (None, None)
        return chem

    try:
        n_cores = max(1, mp.cpu_count() - 1)
        chunksize = max(1, len(chem)//n_cores)
        chunks = [chem.iloc[i:i+chunksize] for i in range(0, len(chem), chunksize)]
        print(f"⚡ {name}: processing {len(chunks)} chunks on {n_cores} cores…")

        results = Parallel(n_jobs=n_cores, backend="loky")(
            delayed(process_chunk)(chunk, name, layer_gdf) for chunk in chunks
        )
        flat = [item for sublist in results for item in sublist]
        chem[[f"{name}_idx", f"{name}_dist_m"]] = flat
        return chem

    except Exception as e:
        print(f"⚠️ Parallel mode failed for {name} → fallback to sequential ({e})")
        errors_path = os.path.join(LOG_DIR, "parallel_fallback.txt")
        with open(errors_path,"a") as f:
            f.write(f"{name} fallback error: {e}\n{traceback.format_exc()}\n\n")

        tqdm.pandas(desc=f"→ {name} (fallback)")
        chem[[f"{name}_idx", f"{name}_dist_m"]] = chem.progress_apply(
            lambda r: nearest_distance(r.geometry, layer_gdf.sindex, layer_gdf.geometry),
            axis=1, result_type="expand"
        )
        return chem

# -----------------------------------------------------------
# Run nearest joins
# -----------------------------------------------------------
print("📏 Running optimized nearest-neighbor join…")
for lname, gdf in layers.items():
    chem = nearest_join_parallel(chem, gdf, lname)

# -----------------------------------------------------------
# Determine overall nearest
# -----------------------------------------------------------
dist_cols = [f"{n}_dist_m" for n in layers]
chem["nearest_type"] = chem[dist_cols].idxmin(axis=1).str.replace("_dist_m","")
chem["nearest_dist_m"] = chem[dist_cols].min(axis=1)

# -----------------------------------------------------------
# Save outputs
# -----------------------------------------------------------
os.makedirs("data_outputs", exist_ok=True)
chem.to_parquet(OUT_PATH, compression="snappy")
preview = chem.sample(min(10000, len(chem)))
preview.to_file("data_outputs/pa_wqx_mine_join_preview.geojson", driver="GeoJSON")

print(f"✅ Saved {len(chem):,} records → {OUT_PATH}")
print("✅ Preview (10k) → data_outputs/pa_wqx_mine_join_preview.geojson")