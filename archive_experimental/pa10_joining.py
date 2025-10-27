#!/usr/bin/env python3
# ===============================================================
# Pennsylvania AMD Step 10 — Unified Spatial Join (Final Build)
# ===============================================================

import geopandas as gpd
import pandas as pd
from pathlib import Path

print("🚀 Starting pa10_joining.py")

# ------------------------------------------------------------------
# 1️⃣ Load cached layers
# ------------------------------------------------------------------
CACHE = Path("data_cache")
OUTS = Path("data_outputs")

def load_layer(fname, label):
    f = CACHE / fname
    if not f.exists():
        print(f"⚠️ Missing {label}: {f}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdf = gpd.read_file(f)
    print(f"✅ Loaded {label}: {len(gdf):,} features")
    return gdf

print("📦 Loading cached layers…")
chem = gpd.read_parquet(OUTS / "pa_wqx_mine_join.parquet")
aml = gpd.read_parquet(CACHE / "AML_AMD_subset.parquet")
bit = load_layer("bituminous.geojson", "Bituminous Mines")
ant = load_layer("anthracite.geojson", "Anthracite Mines")
huc10 = load_layer("HUC10_PA.geojson", "HUC10")
huc12 = load_layer("HUC12_PA.geojson", "HUC12")
flow = load_layer("Flowlines_PA.geojson", "Flowlines")
passive = load_layer("passive.geojson", "Passive Treatment Systems")
print("✅ All layers loaded\n")

# ------------------------------------------------------------------
# 2️⃣ Normalize CRS to EPSG:4326 for all layers
# ------------------------------------------------------------------
def to4326(gdf):
    if not gdf.empty and (gdf.crs is None or gdf.crs.to_epsg() != 4326):
        return gdf.to_crs("EPSG:4326")
    return gdf

chem = to4326(chem)
aml = to4326(aml)
bit = to4326(bit)
ant = to4326(ant)
huc10 = to4326(huc10)
huc12 = to4326(huc12)
flow = to4326(flow)
passive = to4326(passive)

# ------------------------------------------------------------------
# 3️⃣ Containment joins (HUC10 and HUC12)
# ------------------------------------------------------------------
print("🌍 Performing containment joins…")

def safe_sjoin(left, right, cols_map, tag):
    if right.empty:
        print(f"⚠️ Skipping {tag} — empty layer")
        return left
    left.drop(columns=[c for c in left.columns if c.startswith("index_")],
              errors="ignore", inplace=True)
    subset = right[["geometry"] + list(cols_map.keys())].rename(columns=cols_map)
    try:
        merged = gpd.sjoin(left, subset, predicate="within", how="left")
        print(f"✅ Added {tag} fields: {list(cols_map.values())}")
        return merged
    except Exception as e:
        print(f"⚠️ {tag} join failed: {e}")
        return left

chem = safe_sjoin(chem, huc10,
                  {"huc10": "HUC10_ID", "name": "HUC10_NAME"},
                  "HUC10")
chem = safe_sjoin(chem, huc12,
                  {"huc12": "HUC12_ID", "name": "HUC12_NAME"},
                  "HUC12")

# ------------------------------------------------------------------
# 4️⃣ Nearest joins (AML, mines, flowlines, passive)
# ------------------------------------------------------------------
def nearest_join(left, right, label, dist_col, maxdist=250):
    if right.empty:
        print(f"⚠️ Skipping {label} — empty layer")
        return left
    left.drop(columns=[c for c in left.columns if c.startswith("index_")],
              errors="ignore", inplace=True)
    print(f"📏 Computing nearest {label} within {maxdist} m…")
    try:
        joined = gpd.sjoin_nearest(left, right, how="left",
                                   max_distance=maxdist, distance_col=dist_col)
        print(f"✅ Joined {label} ({len(joined):,} records)")
        return joined
    except Exception as e:
        print(f"⚠️ {label} nearest join failed: {e}")
        return left

chem = nearest_join(chem, aml, "AML (AMD subset)", "AML_dist_m")
chem = nearest_join(chem, bit, "Bituminous Mines", "Bituminous_dist_m")
chem = nearest_join(chem, ant, "Anthracite Mines", "Anthracite_dist_m")
chem = nearest_join(chem, flow, "Flowlines", "Flow_dist_m")
chem = nearest_join(chem, passive, "Passive Systems", "Passive_dist_m")

# ------------------------------------------------------------------
# 5️⃣ Write final merged output
# ------------------------------------------------------------------
OUT = OUTS / "PA_wq_joined_mine_hydro.parquet"
chem.to_parquet(OUT, index=False)
print(f"💾 Unified dataset written → {OUT}")

# Preview summary
gdf = gpd.read_parquet(OUT)
print(f"✅ Records: {len(gdf):,}")
print(f"✅ CRS: {gdf.crs}")
print("📄 Sample columns:", list(gdf.columns)[:20])
print("🎯 Step 10 complete.")