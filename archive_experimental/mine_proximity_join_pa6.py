#!/usr/bin/env python3
"""
mine_proximity_join_pa6.py
Optimized Step 4 – Pennsylvania AMD workflow
Joins WQX chemistry points to nearest mine/AML features with:
- parallel R-tree acceleration and automatic fallback
- checkpointing every 50k records (append mode)
- auto resume and cleanup of checkpoints on success
- detailed QC logs
Author: Ashley Mitchell, DOI–OSMRE (2025)
"""

import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.strtree import STRtree
from shapely.ops import nearest_points
from shapely import speedups
from tqdm import tqdm
from joblib import Parallel, delayed
import traceback

speedups.enable()

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
CHEM_PATH = "data_raw/chemistry/wqx_pa_sites_merged_clean.parquet"
AML_PATH = "data_cache/aml_inventory_AMDsubset.geojson"
BIT_PATH = "data_cache/bituminous_surface.geojson"
ANT_PATH = "data_cache/anthracite_surface.geojson"

OUT_PATH = "data_outputs/pa_wqx_mine_join.parquet"
CHECKPOINT_DIR = "data_outputs/checkpoints"
LOG_DIR = "logs"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

QC_LOG = os.path.join(LOG_DIR, "distance_errors_qc.txt")

CHUNK_SIZE = 50000
N_JOBS = max(1, os.cpu_count() - 1)
CRS = "EPSG:26917"

# ---------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------
def log_error(msg):
    with open(QC_LOG, "a") as f:
        f.write(msg + "\n")

def safe_load_geojson(path, label):
    if not os.path.exists(path):
        print(f"⚠️ {label} missing at {path}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
    try:
        gdf = gpd.read_file(path)
        print(f"✅ Loaded {label}: {len(gdf):,} features")
        return gdf
    except Exception as e:
        print(f"❌ Failed to load {label}: {e}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

def load_checkpoints():
    files = sorted([f for f in os.listdir(CHECKPOINT_DIR) if f.endswith(".parquet")])
    return [os.path.join(CHECKPOINT_DIR, f) for f in files]

def append_to_output(df):
    """Append chunk to master parquet file."""
    if os.path.exists(OUT_PATH):
        existing = gpd.read_parquet(OUT_PATH)
        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_parquet(OUT_PATH, compression="snappy")
    else:
        df.to_parquet(OUT_PATH, compression="snappy")

# ---------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------
print("📂 Loading chemistry dataset…")
chem = pd.read_parquet(CHEM_PATH)
chem = gpd.GeoDataFrame(
    chem,
    geometry=gpd.points_from_xy(chem["LongitudeMeasure"], chem["LatitudeMeasure"]),
    crs="EPSG:4326"
).to_crs(CRS)
print(f"✅ Chemistry records: {len(chem):,}")

layers = {
    "AML": safe_load_geojson(AML_PATH, "AML Inventory"),
    "Bituminous": safe_load_geojson(BIT_PATH, "Bituminous Surface Mines"),
    "Anthracite": safe_load_geojson(ANT_PATH, "Anthracite Surface Mines"),
}

for name, gdf in layers.items():
    if not gdf.empty:
        gdf.to_crs(CRS, inplace=True)

# ---------------------------------------------------------------------
# NEAREST SEARCH
# ---------------------------------------------------------------------
def compute_nearest_chunk(df_chunk, target_gdf, name):
    """Compute nearest distance for a chunk of points."""
    try:
        tree = STRtree(target_gdf.geometry)
        geoms = list(target_gdf.geometry)
        idxs = {id(g): i for i, g in enumerate(geoms)}

        results = []
        for geom in df_chunk.geometry:
            try:
                if geom is None or geom.is_empty:
                    results.append((None, np.nan))
                    continue
                nearest = tree.nearest(geom)
                dist = geom.distance(nearest)
                results.append((idxs[id(nearest)], dist))
            except Exception as e:
                log_error(f"{name}: {type(e).__name__} - {e}")
                results.append((None, np.nan))
        return pd.DataFrame(results, columns=[f"{name}_idx", f"{name}_dist_m"])
    except Exception as e:
        log_error(f"{name} chunk-level error: {traceback.format_exc()}")
        return pd.DataFrame(np.nan, index=np.arange(len(df_chunk)), columns=[f"{name}_idx", f"{name}_dist_m"])

# ---------------------------------------------------------------------
# MAIN WORKFLOW
# ---------------------------------------------------------------------
completed = load_checkpoints()
start_idx = 0

if completed:
    last_cp = max(completed, key=os.path.getmtime)
    start_idx = int(os.path.basename(last_cp).split("_")[1].split(".")[0])
    print(f"🔁 Resuming from checkpoint {start_idx:,}")

print("📏 Running optimized nearest-neighbor join…")

for name, gdf in layers.items():
    if gdf.empty:
        print(f"⚠️ Skipping {name} — empty dataset")
        continue

    print(f"⚡ {name}: processing with {N_JOBS} cores…")
    all_results = []
    chunks = [
        chem.iloc[i:i+CHUNK_SIZE]
        for i in range(start_idx, len(chem), CHUNK_SIZE)
    ]

    for i, chunk in enumerate(tqdm(chunks, desc=f"→ {name}")):
        try:
            # Parallel nearest calc
            chunk_splits = np.array_split(chunk, N_JOBS)
            results = Parallel(n_jobs=N_JOBS)(
                delayed(compute_nearest_chunk)(split, gdf, name)
                for split in chunk_splits
            )
            merged = pd.concat(results, ignore_index=True)
        except Exception as e:
            print(f"⚠️ Parallel failed for {name} chunk {i}, fallback sequential.")
            log_error(f"{name} chunk {i} fallback: {e}")
            merged = compute_nearest_chunk(chunk, gdf, name)

        chunk_out = pd.concat([chunk.reset_index(drop=True), merged], axis=1)
        checkpoint_path = os.path.join(CHECKPOINT_DIR, f"{name}_chunk_{i*CHUNK_SIZE}.parquet")
        chunk_out.to_parquet(checkpoint_path, compression="snappy")
        append_to_output(chunk_out)
        all_results.append(chunk_out)

    print(f"✅ {name} layer processed.")
    del all_results

# ---------------------------------------------------------------------
# CLEANUP CHECKPOINTS
# ---------------------------------------------------------------------
print("🧹 Cleaning up checkpoints…")
for f in os.listdir(CHECKPOINT_DIR):
    try:
        os.remove(os.path.join(CHECKPOINT_DIR, f))
    except Exception as e:
        log_error(f"cleanup error {f}: {e}")

print(f"✅ Final results saved to {OUT_PATH}")
print("🎯 Processing complete.")