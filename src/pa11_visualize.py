import geopandas as gpd
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from keplergl import KeplerGl
import orjson
import warnings
import numpy as np
import time
import os

warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)
t0 = time.time()

print("🗺️  Starting pa11_visualize.py")

# === Load unified dataset ===
path = "data_outputs/PA_wq_joined_mine_hydro.parquet"
df = gpd.read_parquet(path)
print(f"✅ Loaded {len(df):,} records for mapping")

# === Clean geometries (vectorized) ===
if "geometry" not in df.columns:
    raise ValueError("❌ No geometry column found in dataset.")

empty_geom = df.geometry.is_empty | df.geometry.isna()
if empty_geom.sum() > 0:
    print(f"⚠️ Found {empty_geom.sum():,} empty geometries — dropping.")
    df = df.loc[~empty_geom]

# === Ensure CRS ===
if df.crs is None or df.crs.to_epsg() != 4326:
    df = df.to_crs(epsg=4326)
    print("🌍 Reprojected to EPSG:4326")

# === Downsample ===
sample_n = 20000 if len(df) > 20000 else len(df)
df_sample = df.sample(n=sample_n, random_state=42)
print(f"🎯 Using {sample_n:,} points for preview map")

# === Compute centroid ===
bounds = df_sample.total_bounds  # [minx, miny, maxx, maxy]
center_lat, center_lon = (bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2

# === Folium Visualization ===
try:
    print("🎨 Building Folium map...")
    m = folium.Map(location=[center_lat, center_lon], zoom_start=7, tiles="CartoDB positron")

    cluster = MarkerCluster(name="Mine Features", disableClusteringAtZoom=10).add_to(m)

    # Vectorized conversion to coordinate arrays (fast)
    coords = np.array([(geom.y, geom.x) for geom in df_sample.geometry if geom.geom_type == "Point"])
    for lat, lon in coords:
        folium.CircleMarker(
            location=[lat, lon],
            radius=2,
            color="#FF4F00",
            fill=True,
            fill_opacity=0.5,
        ).add_to(cluster)

    folium.LayerControl().add_to(m)
    folium_out = "data_outputs/PA_AMD_map.html"
    m.save(folium_out)
    print(f"✅ Folium map saved → {folium_out}")
except Exception as e:
    print(f"❌ Folium map failed: {e}")

# === Kepler.gl Visualization ===
try:
    print("🛰️ Building Kepler.gl map...")

    # Convert datetime and NaT to string for JSON compatibility
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str).replace("NaT", None)

    # Convert any leftover non-serializable types
    df = df.replace({pd.NaT: None, np.nan: None})

    # Sample for performance
    df_kepler = df.sample(n=min(100000, len(df)), random_state=42).copy()

    # Explicitly drop non-serializable columns (geometry handled internally)
    for c in df_kepler.columns:
        if any(isinstance(v, (list, dict, bytes, complex)) for v in df_kepler[c].dropna()[:10]):
            df_kepler.drop(columns=[c], inplace=True, errors="ignore")

    # Build Kepler.gl map
    map_k = KeplerGl(height=800, data={"PA_AMD": df_kepler})
    kepler_path = "data_outputs/PA_AMD_kepler.html"
    map_k.save_to_html(file_name=kepler_path)
    print(f"✅ Kepler.gl map saved → {kepler_path}")

except Exception as e:
    import traceback
    print(f"❌ Kepler.gl map failed: {e}")
    traceback.print_exc()

# === Wrap up ===
t1 = time.time()
print(f"🏁 Visualization pipeline completed in {t1 - t0:.2f} seconds.")	