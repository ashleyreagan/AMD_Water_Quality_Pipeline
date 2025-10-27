# ===============================================================
# Step 9B — Passive Treatment Systems Integration
# ===============================================================

from pathlib import Path
import geopandas as gpd

print("\n💧 Integrating passive treatment systems...")

src_path = Path("data_raw/treatment/passive.shp")
cache_path = Path("data_cache/passive.geojson")

if not src_path.exists():
    print("⚠️ Passive shapefile not found at data_raw/treatment/passive.shp")
    if not cache_path.exists():
        # make empty cache if truly missing
        gpd.GeoDataFrame(geometry=[], crs="EPSG:4326").to_file(cache_path, driver="GeoJSON")
        print("🪣 Created empty passive.geojson placeholder")
else:
    # load shapefile
    passive = gpd.read_file(src_path)
    print(f"✅ Loaded passive shapefile: {len(passive):,} features")

    # normalize geometry CRS
    if passive.crs is None or passive.crs.to_epsg() != 4326:
        passive = passive.to_crs("EPSG:4326")

    # optional: find a usable name/ID field
    name_field = None
    for c in passive.columns:
        if any(k in c.lower() for k in ["name", "system", "site", "id"]):
            name_field = c
            break

    if name_field:
        passive.rename(columns={name_field: "PASSIVE_NAME"}, inplace=True)
        print(f"🪪 Using '{name_field}' as PASSIVE_NAME")

    # write to cache
    passive.to_file(cache_path, driver="GeoJSON")
    print(f"💾 Cached passive systems → {cache_path}")

print("✅ Passive system integration complete.")