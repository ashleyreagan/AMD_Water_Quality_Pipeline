# =============================================================
# Watershed Stats v1.6 — Full-Height Safari-Compatible Dashboard
# by Ashley R. Mitchell / DOI OSMRE
# =============================================================

import streamlit as st
import geopandas as gpd
import pandas as pd
import folium
from streamlit.components.v1 import html
from pathlib import Path
import subprocess
import time
import warnings

# -------------------------------------------------------------
# Silence Shapely geometry warnings
warnings.filterwarnings("ignore", category=RuntimeWarning, module="shapely")

# -------------------------------------------------------------
# Paths
DATA_DIR = Path("data_outputs")
CACHE_DIR = Path("data_cache")
HUC12_FILE = CACHE_DIR / "HUC12_PA.geojson"
SCRIPT = Path("owl_package.py")

# -------------------------------------------------------------
# Cached data loaders
@st.cache_data
def load_huc12_list():
    gdf = gpd.read_file(HUC12_FILE)
    cols = [c for c in gdf.columns if "HUC12" in c.upper()]
    huc_col = cols[0] if cols else gdf.columns[0]
    name_col = "NAME" if "NAME" in gdf.columns else huc_col
    return gdf[[huc_col, name_col]]

@st.cache_data
def load_summary():
    f = DATA_DIR / "owl_wqp_summary.csv"
    return pd.read_csv(f) if f.exists() else pd.DataFrame()

@st.cache_data
def load_pasda_layers():
    layers = []
    for f in CACHE_DIR.glob("*.geojson"):
        try:
            gdf = gpd.read_file(f)
            layers.append((f.stem, gdf))
        except Exception as e:
            st.warning(f"Could not read {f.name}: {e}")
    return layers

@st.cache_data
def load_wqp_points():
    """Load available WQP result CSVs for mapping."""
    chem_layers = {}
    color_map = {
        "iron_latest.csv": "orange",
        "manganese_latest.csv": "purple",
        "ph_latest.csv": "blue",
        "sulfate_latest.csv": "green",
        "temperature_water_latest.csv": "red",
        "specific_conductance_latest.csv": "gray",
    }
    for csv_file, color in color_map.items():
        f = DATA_DIR / csv_file
        if not f.exists():
            continue
        try:
            df = pd.read_csv(f)
            if {"LatitudeMeasure", "LongitudeMeasure"}.issubset(df.columns):
                gdf = gpd.GeoDataFrame(
                    df,
                    geometry=gpd.points_from_xy(df["LongitudeMeasure"], df["LatitudeMeasure"]),
                    crs="EPSG:4326"
                )
                chem_layers[csv_file.replace("_latest.csv", "")] = (gdf, color)
        except Exception as e:
            st.warning(f"Error loading {csv_file}: {e}")
    return chem_layers

# -------------------------------------------------------------
# Run Owl Creek engine for selected HUC12
def run_wqp_fetch(huc12):
    st.info(f"Running Owl Creek engine for HUC12 {huc12} …")
    start = time.time()
    cmd = ["python", str(SCRIPT), str(huc12)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    runtime = (time.time() - start) / 60
    st.success(f"Runtime: {runtime:.2f} min")
    st.text_area("Execution Log", result.stdout + result.stderr, height=300)

# -------------------------------------------------------------
# Map builder
def build_map(show_layers, simplify_tol=0.001, max_features=5000):
    """Build efficient Folium map with simplified PASDA and selected WQP layers."""
    m = folium.Map(location=[40.0, -78.5], zoom_start=8)

    # PASDA Layers
    layer_colors = {
        "flow": "blue",
        "huc": "gray",
        "bituminous": "brown",
        "anthracite": "black",
        "passive": "green",
        "aml": "orange",
        "underground": "purple",
    }

    for name, gdf in load_pasda_layers():
        if len(gdf) > max_features:
            continue

        for col in gdf.columns:
            if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                gdf[col] = gdf[col].astype(str)

        gdf = gdf.to_crs(4326)
        gdf["geometry"] = gdf["geometry"].simplify(simplify_tol, preserve_topology=True)

        color = next((val for key, val in layer_colors.items() if key in name.lower()), "darkred")
        tooltip_fields = [c for c in gdf.columns if c != gdf.geometry.name][:5]

        folium.GeoJson(
            gdf,
            name=name,
            tooltip=folium.GeoJsonTooltip(fields=tooltip_fields),
            style_function=lambda x, color=color: {
                "color": color,
                "weight": 1,
                "fillOpacity": 0.15,
            },
        ).add_to(m)

    # WQP Layers
    chem_layers = load_wqp_points()
    for chem_name, (gdf, color) in chem_layers.items():
        if chem_name not in show_layers or not show_layers[chem_name]:
            continue
        group = folium.FeatureGroup(name=f"{chem_name.title()} samples")
        for _, row in gdf.iterrows():
            lat, lon = row.geometry.y, row.geometry.x
            popup = "<br>".join(
                f"{c}: {row[c]}" for c in row.index if c not in ["geometry"]
            )[:800]
            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color=color,
                fill=True,
                fill_opacity=0.7,
                popup=folium.Popup(popup, max_width=300),
            ).add_to(group)
        group.add_to(m)

    folium.LayerControl().add_to(m)
    return m

# -------------------------------------------------------------
# Streamlit UI
st.set_page_config(
    page_title="Watershed Stats",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🧭"
)

st.title("🧭 Watershed Stats — Interactive HUC12 Dashboard")
st.caption("Powered by Owl Creek v4.2 • PASDA + WQP (Iron, Manganese, pH, Sulfate, etc.)")

# Sidebar Controls
st.sidebar.header("Watershed Controls")
huc12_df = load_huc12_list()
huc_choice = st.sidebar.selectbox("Select HUC12 watershed", huc12_df.iloc[:, 0])
fetch_new = st.sidebar.button("Fetch / Refresh Data")

st.sidebar.markdown("---")
st.sidebar.subheader("Chemical Layers")
show_layers = {
    "iron": st.sidebar.checkbox("Iron (Fe)", value=True),
    "manganese": st.sidebar.checkbox("Manganese (Mn)", value=False),
    "ph": st.sidebar.checkbox("pH", value=True),
    "sulfate": st.sidebar.checkbox("Sulfate (SO₄)", value=True),
    "temperature_water": st.sidebar.checkbox("Temperature, water (°C)", value=False),
    "specific_conductance": st.sidebar.checkbox("Specific conductance (µS/cm)", value=False),
}

if fetch_new:
    run_wqp_fetch(huc_choice)

# -------------------------------------------------------------
# Summary Table
st.subheader(f"HUC12 Summary for `{huc_choice}`")
df = load_summary()
if not df.empty:
    st.dataframe(df.head(50), width="stretch")
else:
    st.info("No WQP summary found yet — run a fetch first.")

# -------------------------------------------------------------
# Interactive Map (Full-Height Safari Fix)
st.subheader("🗺️ Interactive Map")

map_obj = build_map(show_layers)

# Save Folium map to temporary HTML and embed directly
map_html_path = DATA_DIR / "map_embed.html"
map_obj.save(map_html_path)
with open(map_html_path, "r", encoding="utf-8") as f:
    folium_html = f.read()

# Embed map directly with full-page responsive height
html(
    f"""
    <style>
    .map-container {{
        width: 100%;
        height: calc(100vh - 150px);
        overflow: hidden;
        border-radius: 8px;
    }}
    iframe {{
        width: 100%;
        height: 100%;
        border: none;
    }}
    </style>
    <div class="map-container">
        {folium_html}
    </div>
    """,
    height=1000,
    width="100%",
)

st.markdown("---")
st.caption(
    "⚙️ Safari-safe map rendering • PASDA layers simplified in memory • "
    "WQP overlays are point-based and toggleable by parameter type."
)