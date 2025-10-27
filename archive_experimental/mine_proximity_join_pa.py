#!/usr/bin/env python3
"""
Mine Proximity Join (Pennsylvania)
----------------------------------
Joins WQX chemistry sites to nearest mining and AML features
using cleaned local GeoJSON data.

Developed by Ashley Mitchell, DOI–OSMRE, 2025
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import os
from shapely.geometry import Point
from tqdm import tqdm

# ============================================================
# CONFIG
# ============================================================

DATA_DIR = "data_cache"
CHEM_PATH = "data_raw/chemistry/wqx_pa_sites_merged_clean.csv"
OUT_PATH = "data_outputs/wqx_pa_mine_proximity.csv"

# Define local mine layers
LAYER_PATHS = {
    "anthracite_surface": os.path.join(DATA_DIR, "anthracite_surface.geojson"),
    "bituminous_surface": os.path.join(DATA_DIR, "bituminous_surface.geojson"),
    "underground": os.path.join(DATA_DIR, "underground.geojson"),
    "aml_inventory": os.path.join(DATA_DIR, "aml_inventory_AMDsubset.geojson"),  # auto-uses filtered AMD
}

# Coordinate system: NAD83 / UTM zone 17N
TARGET_CRS = "EPSG:26917"
SEARCH_RADIUS_M = 5000  # meters

# ============================================================
# HELPERS
# ============================================================

def log(msg):
    print(f"{msg}")

def load_mine_layer(name, path):
    """Load local GeoJSON or skip if missing."""
    if not os.path.exists(path):
        log(f"⚠️ Missing layer: {name} ({path})")
        return None
    try:
        gdf = gpd.read_file(path)
        log(f"✅ Loaded {len(gdf)} features from {name}")
        return gdf
    except Exception as e:
        log(f"❌ Failed to load {name}: {e}")
        return None

def load_wqx_sites(path):
    """Load WQX CSV safely: tolerant parsing, pick only lat/lon (+ID if present),
    coerce to numeric, drop invalid coords, return GeoDataFrame (EPSG:4326)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"WQX file not found: {path}")

    log(f"📂 Loading WQX CSV from {path}")

    # 1) Read header only to detect real column names
    hdr = pd.read_csv(path, nrows=0, engine="python")
    cols = list(hdr.columns)

    # Candidate names seen in WQP exports
    lat_candidates = ["LatitudeMeasure", "Latitude", "MonitoringLocationLatitude", "DEC_LAT", "Lat"]
    lon_candidates = ["LongitudeMeasure", "Longitude", "MonitoringLocationLongitude", "DEC_LONG", "Lon"]
    id_candidates  = ["MonitoringLocationIdentifier", "ActivityIdentifier", "OrganizationIdentifier"]

    lat_col = next((c for c in lat_candidates if c in cols), None)
    lon_col = next((c for c in lon_candidates if c in cols), None)
    id_col  = next((c for c in id_candidates  if c in cols), None)

    if not lat_col or not lon_col:
        raise ValueError(f"Could not find latitude/longitude columns in CSV. "
                         f"Seen columns: {cols[:10]} … ({len(cols)} total)")

    usecols = [lat_col, lon_col] + ([id_col] if id_col else [])

    # 2) Tolerant full read: skip malformed lines, keep as strings
    df = pd.read_csv(
        path,
        engine="python",          # tolerant parser
        on_bad_lines="skip",      # skip broken rows
        dtype=str,                # avoid mixed-type issues
        usecols=usecols,          # only what we need
    )

    # 3) Coerce to numeric and drop invalid coords
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    before = len(df)
    df = df.dropna(subset=[lat_col, lon_col])
    dropped = before - len(df)
    if dropped:
        log(f"⚠️ Dropped {dropped} rows with invalid coordinates (non-numeric or missing)")

    # 4) Build GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326"
    )

    kept = [c for c in [id_col, lat_col, lon_col] if c]
    log(f"✅ Loaded {len(gdf)} WQX sites with columns {kept}")
    return gdf
# ============================================================
# MAIN
# ============================================================

def main():
    log("=== Mine Proximity Join Started ===")

    # Load WQX sites
    sites = load_wqx_sites(CHEM_PATH).to_crs(TARGET_CRS)

    # Load each mine/AML layer
    mine_layers = {}
    for name, path in LAYER_PATHS.items():
        gdf = load_mine_layer(name, path)
        if gdf is not None and len(gdf) > 0:
            mine_layers[name] = gdf.to_crs(TARGET_CRS)

    if not mine_layers:
        log("❌ No mine layers available. Exiting.")
        return

    # Compute proximity
    results = []
    for name, mines in mine_layers.items():
        log(f"🔍 Evaluating proximity for {name} ({len(mines)} features)")
        try:
            joined = gpd.sjoin_nearest(
                sites, mines,
                how="left",
                max_distance=SEARCH_RADIUS_M,
                distance_col=f"{name}_dist_m"
            )
            results.append(joined[[f"{name}_dist_m"]])
            log(f"✅ Joined {len(joined)} rows with {name}")
        except Exception as e:
            log(f"⚠️ Failed proximity join for {name}: {e}")

    # Combine distances
    log("🧩 Combining proximity results...")
    
    # Reset index to ensure unique alignment
    sites = sites.reset_index(drop=True)

    # Concatenate all result DataFrames horizontally
    for res in results:
        res = res.reset_index(drop=True)
        sites = pd.concat([sites, res], axis=1, ignore_index=False)

    # Compute min distance and dominant source
    dist_cols = [col for col in sites.columns if col.endswith("_dist_m")]
    if not dist_cols:
        log("⚠️ No distance columns found — skipping final summary.")
    else:
        sites["nearest_mine_type"] = (
            sites[dist_cols].idxmin(axis=1).str.replace("_dist_m", "", regex=False)
        )
        sites["nearest_mine_dist_m"] = sites[dist_cols].min(axis=1)

    # Save output
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    sites.drop(columns=["geometry"]).to_csv(OUT_PATH, index=False)
    log(f"💾 Results saved to {OUT_PATH}")
    log("✅ Mine proximity join complete.")
# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()