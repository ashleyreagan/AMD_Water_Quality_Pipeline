#!/usr/bin/env python3
"""
mine_proximity_join_pa8.py
Optimized + fault-tolerant version of Step 4 – Pennsylvania AMD workflow

Fixes:
 • Correct checkpoint path handling
 • Automatically resets/resumes safely if checkpoint files are missing
 • Logs and skips broken chunks gracefully
 • Fully suppresses noisy warnings

Author: Ashley Mitchell, DOI–OSMRE (2025)
"""

import os
import sys
import warnings
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.strtree import STRtree
from joblib import Parallel, delayed
from tqdm import tqdm
import traceback

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

CHEM_PATH = "data_raw/chemistry/wqx_pa_sites_merged_clean.parquet"
AML_PATH  = "data_cache/aml_inventory_AMDsubset.geojson"
BIT_PATH  = "data_cache/bituminous_surface.geojson"
ANT_PATH  = "data_cache/anthracite_surface.geojson"

OUT_PATH = "data_outputs/pa_wqx_mine_join.parquet"
CHECKPOINT_DIR = "data_outputs/checkpoints"
LOG_DIR = "logs"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

QC_LOG = os.path.join(LOG_DIR, "distance_errors_qc.txt")

CHUNK_SIZE = 30000
N_JOBS = min(8, os.cpu_count() - 1)
CRS = "EPSG:26917"

# ---------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------
def log_error(msg):
    with open(QC_LOG, "a") as f:
        f.write(msg + "\n")

def safe_load_geojson(path, label):
    """Load GeoJSON or return empty GeoDataFrame."""
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

def append_to_output(df):
    """Append chunk to master parquet file (safe merge)."""
    if os.path.exists(OUT_PATH):
        existing = gpd.read_parquet(OUT_PATH)
        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_parquet(OUT_PATH, compression="snappy")
    else:
        df.to_parquet(OUT_PATH, compression="snappy")

def latest_checkpoint():
    """Return latest checkpoint index or 0 if none or broken."""
    try:
        files = [f for f in os.listdir(CHECKPOINT_DIR) if f.endswith(".parquet")]
        if not files:
            return 0
        latest = max(files, key=lambda f: os.path.getmtime(os.path.join(CHECKPOINT_DIR, f)))
        full_path = os.path.join(CHECKPOINT_DIR, latest)
        if not os.path.exists(full_path):
            print("⚠️ Checkpoint reference invalid, starting fresh.")
            return 0
        return int(latest.split("_")[-1].split(".")[0])
    except Exception as e:
        print(f"⚠️ Checkpoint detection failed: {e}")
        return 0

def reset_checkpoints():
    for f in os.listdir(CHECKPOINT_DIR):
        try:
            os.remove(os.path.join(CHECKPOINT_DIR, f))
        except Exception as e:
            log_error(f"cleanup {f}: {e}")

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

for gdf in layers.values():
    if not gdf.empty:
        gdf.to_crs(CRS, inplace=True)

# ---------------------------------------------------------------------
# CORE NEAREST SEARCH
# ---------------------------------------------------------------------
def compute_nearest_chunk(df_chunk, target_gdf, name):
    """Compute nearest distance for a chunk of points using STRtree."""
    try:
        tree = STRtree(target_gdf.geometry)
        geoms = list(target_gdf.geometry)
        idx_map = {id(g): i for i, g in enumerate(geoms)}

        idxs, dists = [], []
        for geom in df_chunk.geometry:
            try:
                if geom is None or geom.is_empty:
                    idxs.append(None)
                    dists.append(np.nan)
                    continue
                nearest = tree.nearest(geom)
                idxs.append(idx_map[id(nearest)])
                dists.append(geom.distance(nearest))
            except Exception as e:
                log_error(f"{name} error: {type(e).__name__} - {e}")
                idxs.append(None)
                dists.append(np.nan)
        return pd.DataFrame({f"{name}_idx": idxs, f"{name}_dist_m": dists})
    except Exception as e:
        log_error(f"{name} chunk failure: {traceback.format_exc()}")
        return pd.DataFrame(np.nan, index=np.arange(len(df_chunk)), columns=[f"{name}_idx", f"{name}_dist_m"])

# ---------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------
start_idx = latest_checkpoint()
if start_idx > 0:
    print(f"🔁 Resuming from checkpoint {start_idx:,}")
else:
    print("🚀 Starting fresh join process")
    reset_checkpoints()

for name, gdf in layers.items():
    if gdf.empty:
        print(f"⚠️ Skipping {name} — no data")
        continue

    print(f"⚡ {name}: running with {N_JOBS} cores, chunk size {CHUNK_SIZE:,}")
    chunks = [chem.iloc[i:i+CHUNK_SIZE] for i in range(start_idx, len(chem), CHUNK_SIZE)]

    for i, chunk in enumerate(tqdm(chunks, desc=f"→ {name}")):
        checkpoint_file = os.path.join(CHECKPOINT_DIR, f"{name}_chunk_{i*CHUNK_SIZE}.parquet")
        if os.path.exists(checkpoint_file):
            continue  # skip already done

        try:
            splits = np.array_split(chunk, N_JOBS)
            results = Parallel(n_jobs=N_JOBS)(
                delayed(compute_nearest_chunk)(s, gdf, name) for s in splits
            )
            merged = pd.concat(results, ignore_index=True)
        except Exception as e:
            print(f"⚠️ {name} parallel fail on chunk {i}, switching to sequential…")
            log_error(f"{name} fallback chunk {i}: {e}")
            merged = compute_nearest_chunk(chunk, gdf, name)

        out_chunk = pd.concat([chunk.reset_index(drop=True), merged], axis=1)
        out_chunk.to_parquet(checkpoint_file, compression="snappy")
        append_to_output(out_chunk)

    print(f"✅ Finished {name}")

# ---------------------------------------------------------------------
# CLEANUP
# ---------------------------------------------------------------------
print("🧹 Cleaning up checkpoints…")
reset_checkpoints()
print(f"✅ Results written to {OUT_PATH}")
print("🎯 Processing complete.")