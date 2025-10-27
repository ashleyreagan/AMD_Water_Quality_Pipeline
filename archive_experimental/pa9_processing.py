#!/usr/bin/env python3
# ===============================================================
# Pennsylvania AMD Step 9 — Unified Processing Pipeline (Final Fixed Build)
# ===============================================================

import geopandas as gpd
import pandas as pd
import requests, io, re, time
from pathlib import Path

print("🚀 Starting pa9_processing.py")

# ------------------------------------------------------------------
# 1️⃣ Paths
# ------------------------------------------------------------------
CACHE = Path("data_cache")
CACHE.mkdir(exist_ok=True)
OUTS = Path("data_outputs")
OUTS.mkdir(exist_ok=True)

# ------------------------------------------------------------------
# 2️⃣ Load chemistry dataset
# ------------------------------------------------------------------
chem_parquet = OUTS / "pa_wqx_mine_join.parquet"
chem_csv = OUTS / "wqx_pa_sites_merged_AMD_features_spatial.csv"

if chem_parquet.exists():
    chem = gpd.read_parquet(chem_parquet)
elif chem_csv.exists():
    chem = pd.read_csv(chem_csv, low_memory=False)
else:
    raise FileNotFoundError("❌ Chemistry dataset not found in data_outputs/")

# normalize CRS to EPSG:4326 for consistency
try:
    chem = chem.to_crs("EPSG:4326")
except Exception:
    pass
print(f"✅ Chemistry records: {len(chem):,}")

# ------------------------------------------------------------------
# 3️⃣ Load or rebuild AML AMD subset
# ------------------------------------------------------------------
subset_parquet = CACHE / "AML_AMD_subset.parquet"
if subset_parquet.exists():
    aml = gpd.read_parquet(subset_parquet)
else:
    src = CACHE / "aml_inventory.geojson"
    if not src.exists():
        raise SystemExit("❌ Missing AML inventory: data_cache/aml_inventory.geojson")
    gdf = gpd.read_file(src)
    gdf.columns = [c.upper() for c in gdf.columns]
    cand_cols = [c for c in gdf.columns if any(k in c for k in
        ["PROBLEM","KEYWORD","DESC","TYPE","ISSUE","NOTES","NAME"])]
    text = gdf[cand_cols].astype(str).agg(" ".join, axis=1).str.upper()
    pat = re.compile(r"\b(AMD|ACID|DRAINAGE|SEEP|DISCHARGE|MINE WATER|POLLUTION)\b", re.I)
    gdf_amd = gdf[text.str.contains(pat, na=False)].copy()
    gdf_amd.set_geometry("GEOMETRY", inplace=True, crs="EPSG:4326")
    gdf_amd.to_parquet(subset_parquet)
    aml = gdf_amd
print(f"✅ Loaded AML Inventory (AMD subset): {len(aml):,} features")

# ------------------------------------------------------------------
# 4️⃣ Helper: ArcGIS REST fetcher using objectId batching (PASDA-safe)
# ------------------------------------------------------------------
def fetch_arcgis_full(service_url, layer_id, label, out_name):
    """Fetch all records from ArcGIS REST layer via objectId batching (bypasses 1,000 limit)."""
    out_path = CACHE / out_name
    if out_path.exists():
        print(f"✅ Using cached {label}")
        return gpd.read_file(out_path)

    print(f"🌐 Fetching {label} from {service_url}/{layer_id} (objectId batching)…")

    oid_url = f"{service_url}/{layer_id}/query"
    oid_params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
    r = requests.get(oid_url, params=oid_params)
    r.raise_for_status()
    ids = r.json().get("objectIds", [])
    if not ids:
        print(f"❌ No ObjectIDs returned for {label}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    print(f"   → Total records: {len(ids):,}")
    step = 500
    frames = []
    for i in range(0, len(ids), step):
        batch = ids[i:i+step]
        params = {
            "objectIds": ",".join(map(str, batch)),
            "outFields": "*",
            "f": "geojson"
        }
        r = requests.get(f"{service_url}/{layer_id}/query", params=params, timeout=300)
        if r.status_code != 200:
            print(f"⚠️ HTTP {r.status_code} at batch {i//step}")
            continue
        try:
            gtmp = gpd.read_file(io.StringIO(r.text))
            if not gtmp.empty:
                frames.append(gtmp)
            print(f"   pulled {min(i+step, len(ids))}/{len(ids)}", end="\r")
        except Exception as e:
            print(f"⚠️ Batch {i//step} parse error: {e}")
        time.sleep(0.1)

    if not frames:
        print(f"❌ No data retrieved for {label}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    gdf = pd.concat(frames, ignore_index=True)
    gdf.to_file(out_path, driver="GeoJSON")
    print(f"\n✅ Saved {label}: {len(gdf):,} features → {out_path}")
    return gdf

# ------------------------------------------------------------------
# 5️⃣ Fetch mining and hydrologic layers
# ------------------------------------------------------------------
DEP2 = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP2/MapServer"
bit = fetch_arcgis_full(DEP2, 22, "Bituminous Surface Mines", "bituminous.geojson")
ant = fetch_arcgis_full(DEP2, 21, "Anthracite Surface Mines", "anthracite.geojson")

# ------------------------------------------------------------------
# 5️⃣a HUC10 (Hydrologic Units 10-digit)
# ------------------------------------------------------------------
huc10_path = CACHE / "HUC10_PA.geojson"
if huc10_path.exists():
    print("✅ Using cached HUC10_PA")
    huc10 = gpd.read_file(huc10_path)
else:
    print("🌐 Downloading HUC10_PA (2023 Hydrologic Units)…")
    huc10 = gpd.read_file("https://www.pasda.psu.edu/json/PA_HUC10_2023.geojson")
    huc10.to_file(huc10_path, driver="GeoJSON")
    print(f"✅ Saved HUC10_PA: {len(huc10):,} features → {huc10_path}")

# ------------------------------------------------------------------
# 5️⃣b HUC12 (Hydrologic Units 12-digit)
# ------------------------------------------------------------------
huc12_path = CACHE / "HUC12_PA.geojson"
if huc12_path.exists():
    # validate cache
    cols = gpd.read_file(huc12_path, rows=1).columns
    if not any("huc" in c.lower() for c in cols):
        print("🧹 Removing incorrect HUC12 cache (non-hydrologic)")
        huc12_path.unlink(missing_ok=True)

if huc12_path.exists():
    print("✅ Using cached HUC12_PA")
    huc12 = gpd.read_file(huc12_path)
else:
    print("🌐 Downloading HUC12_PA (2023 Hydrologic Units)…")
    url = "https://www.pasda.psu.edu/json/PA_HUC12_2023.geojson"
    try:
        huc12 = gpd.read_file(url)
        huc12.to_file(huc12_path, driver="GeoJSON")
        print(f"✅ Saved HUC12_PA: {len(huc12):,} features → {huc12_path}")
        print(f"🪪 Fields: {list(huc12.columns)}")
    except Exception as e:
        print(f"❌ Error downloading HUC12: {e}")
        huc12 = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

# ------------------------------------------------------------------
# 5️⃣c NHD Flowlines (USGS Hydrology)
# ------------------------------------------------------------------
USGS_HYDRO = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/USGeologicalSurvey/MapServer"
flow = fetch_arcgis_full(USGS_HYDRO, 7, "NHD Flowlines (1999 Medium Resolution)", "Flowlines_PA.geojson")

# ------------------------------------------------------------------
# 6️⃣ Passive treatment systems ingestion
# ------------------------------------------------------------------
print("\n💧 Integrating passive treatment systems...")
src_passive = Path("data_raw/treatment/passive.shp")
cache_passive = CACHE / "passive.geojson"

if not src_passive.exists():
    print("⚠️ Passive shapefile not found; creating placeholder")
    gpd.GeoDataFrame(geometry=[], crs="EPSG:4326").to_file(cache_passive, driver="GeoJSON")
else:
    passive = gpd.read_file(src_passive)
    print(f"✅ Loaded passive shapefile: {len(passive):,} features")
    if passive.crs is None or passive.crs.to_epsg() != 4326:
        passive = passive.to_crs("EPSG:4326")
    name_field = next((c for c in passive.columns if any(k in c.lower() for k in ["name","system","site","id"])), None)
    if name_field:
        passive.rename(columns={name_field: "PASSIVE_NAME"}, inplace=True)
        print(f"🪪 Using '{name_field}' as PASSIVE_NAME")
    passive.to_file(cache_passive, driver="GeoJSON")
    print(f"💾 Cached passive systems → {cache_passive}")
print("✅ Passive system integration complete.")

# ------------------------------------------------------------------
# 7️⃣ Output placeholder
# ------------------------------------------------------------------
out_csv = OUTS / "wqx_pa_sites_merged_AMD_features_spatial.csv"
chem.to_csv(out_csv, index=False)
print(f"💾 Wrote merged placeholder → {out_csv}")
print("🎯 Processing complete.")