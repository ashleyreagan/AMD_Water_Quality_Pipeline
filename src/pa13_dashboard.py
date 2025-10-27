#!/usr/bin/env python3
# ===============================================================
# Pennsylvania AMD Step 13 — Streamlit Dashboard (Robust v3)
# ===============================================================

import geopandas as gpd
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
import re

st.set_page_config(page_title="PA AMD Dashboard", layout="wide")

# ----------------------------- Helpers -----------------------------
EXCLUDE_TOKENS = ["_in", "_out", " in", " out", "inlet", "outlet", "(in", "(out", " inflow", " outflow"]

def drop_in_out(columns):
    cols = []
    for c in columns:
        name = c.lower()
        if any(tok in name for tok in EXCLUDE_TOKENS):
            continue
        cols.append(c)
    return cols

def match_any(name: str, tokens) -> bool:
    n = name.lower()
    return any(t in n for t in tokens)

def pick_best(df: pd.DataFrame, candidates, min_nonnull=1000):
    """Return the column from `candidates` with largest non-null numeric count, else None."""
    best, best_n = None, -1
    for c in candidates:
        if c not in df.columns: 
            continue
        n = pd.to_numeric(df[c], errors="coerce").notna().sum()
        if n > best_n and n >= min_nonnull:
            best, best_n = c, n
    return best

# -------------------------- Load & preprocess --------------------------
@st.cache_data(show_spinner=True)
def load_data(max_rows=250_000):
    p = Path("data_outputs/PA_wq_joined_mine_hydro.parquet")
    if not p.exists():
        st.stop()
    gdf = gpd.read_parquet(p)

    # Ensure WGS84 for mapping; chemistry points are POINT geometries
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    if "geometry" in gdf.columns:
        gdf["LAT"] = gdf.geometry.y
        gdf["LON"] = gdf.geometry.x

    # Downsample for browser payload, keep all columns
    if len(gdf) > max_rows:
        gdf = gdf.sample(max_rows, random_state=42)

    # Non-geometry display copy (for tables/plots)
    df_display = gdf.drop(columns=["geometry"], errors="ignore").copy()

    # Convert object columns to string for safe Arrow serialization in Streamlit
    for c in df_display.columns:
        if df_display[c].dtype == "object":
            df_display[c] = df_display[c].astype(str)

    return gdf, df_display

st.title("💧 Pennsylvania AMD Dashboard — Chemistry & Hydrology Overview")

with st.spinner("Loading unified dataset..."):
    gdf, df_display = load_data()

st.success(f"✅ Loaded {len(df_display):,} rows × {len(df_display.columns):,} fields (sampled if very large).")

# --------------------------- Chemistry sets ---------------------------
# Build inclusive analyte universe, then drop inlet/outlet variants
all_cols = list(df_display.columns)
chem_universe = [c for c in all_cols if match_any(c, [
    "ph", "cond", "specificconductance", "so4", "sulfate", "sulphate",
    "iron", "fe_", "mn", "manganese", "al", "aluminum", "acidity",
    "alkalinity", "tds", "total dissolved solids", "temp", "temperature"
])]
chem_universe = drop_in_out(chem_universe)

# Coverage counts → keep top 10 most populated numeric analytes
coverage_counts = {
    c: pd.to_numeric(gdf[c], errors="coerce").notna().sum() if c in gdf.columns else 0
    for c in chem_universe
}
top_chem = sorted([c for c in chem_universe if coverage_counts.get(c, 0) > 1000],
                  key=lambda c: coverage_counts[c], reverse=True)[:10]

# ------------------------------ Layout ------------------------------
tab_cov, tab_chem, tab_hot, tab_huc, tab_corr = st.tabs(
    ["📋 Analyte Coverage", "🧪 Chemistry Explorer", "🔥 Hotspots", "🌊 HUC Summary", "🔬 Correlations"]
)

# =========================== Analyte Coverage ===========================
with tab_cov:
    st.subheader("📋 Analyte Coverage (non-In/Out variables)")
    cov_df = (pd.DataFrame({"analyte": list(coverage_counts.keys()),
                            "non_null_numeric": list(coverage_counts.values())})
              .sort_values("non_null_numeric", ascending=False))
    st.dataframe(cov_df, use_container_width=True)
    st.caption("Only analytes excluding inlet/outlet variants are listed. Counts reflect numeric-parsable values.")

# =========================== Chemistry Explorer =========================
with tab_chem:
    st.subheader("🧪 Chemistry Explorer (Top coverage analytes)")
    if not top_chem:
        st.warning("No analytes exceed the coverage threshold. Try lowering the threshold or check input columns.")
    else:
        sel = st.selectbox("Select analyte", top_chem,
                           format_func=lambda c: f"{c}  (n={coverage_counts[c]:,})")
        series = pd.to_numeric(gdf[sel], errors="coerce")
        numeric = series.dropna()
        if numeric.empty:
            st.warning(f"No numeric data available for {sel}.")
        else:
            stats = numeric.describe().to_frame().T
            st.dataframe(stats.style.format("{:.2f}"))

            fig, axes = plt.subplots(1, 2, figsize=(10, 4))
            sns.histplot(numeric, kde=True, ax=axes[0], color="steelblue")
            axes[0].set_title(f"Histogram: {sel}")
            sns.boxplot(x=numeric, ax=axes[1], color="orange")
            axes[1].set_title(f"Boxplot: {sel}")
            st.pyplot(fig)

# ================================ Hotspots ==============================
with tab_hot:
    st.subheader("🔥 AMD Hotspots")

    # Robust pickers: avoid *in/*out; prefer broad fields
    ph_candidates = [c for c in chem_universe if "ph" in c.lower()]
    fe_candidates = [c for c in chem_universe if ("iron" in c.lower() or c.lower().startswith("fe"))]

    ph_col = pick_best(gdf, ph_candidates, min_nonnull=2000)  # require some coverage
    fe_col = pick_best(gdf, fe_candidates, min_nonnull=2000)

    # Fallbacks if Fe/pH absent: sulfate / conductivity
    so4_candidates = [c for c in chem_universe if ("so4" in c.lower() or "sulfate" in c.lower())]
    cond_candidates = [c for c in chem_universe if ("cond" in c.lower() or "specificconductance" in c.lower())]
    so4_col = pick_best(gdf, so4_candidates, min_nonnull=2000)
    cond_col = pick_best(gdf, cond_candidates, min_nonnull=2000)

    st.write("Selected fields:",
             f"pH → **{ph_col or 'None'}**; Fe → **{fe_col or 'None'}**; "
             f"Sulfate → **{so4_col or 'None'}**; Conductivity → **{cond_col or 'None'}**")

    # Threshold controls
    col1, col2, col3 = st.columns(3)
    with col1:
        ph_thr = st.number_input("pH threshold (acidic <)", value=4.0, step=0.1)
    with col2:
        fe_thr = st.number_input("Fe threshold mg/L (>)", value=10.0, step=0.5)
    with col3:
        so4_thr = st.number_input("Sulfate mg/L fallback (>)", value=250.0, step=10.0)

    # Build mask with graceful fallback order
    mask = pd.Series(False, index=gdf.index)
    if ph_col is not None:
        mask |= (pd.to_numeric(gdf[ph_col], errors="coerce") < ph_thr)
    if fe_col is not None:
        mask |= (pd.to_numeric(gdf[fe_col], errors="coerce") > fe_thr)

    # If neither pH nor Fe had enough data to flag anything, try sulfate/cond as proxies
    if not mask.any():
        if so4_col is not None:
            mask |= (pd.to_numeric(gdf[so4_col], errors="coerce") > so4_thr)
        elif cond_col is not None:
            # very generic high conductivity proxy for AMD
            mask |= (pd.to_numeric(gdf[cond_col], errors="coerce") > 500)

    hot = gdf.loc[mask].copy()
    st.info(f"Flagged {len(hot):,} hotspot records using selected/fallback analytes.")

    # Map requires lat/lon
    if len(hot) and {"LAT", "LON"}.issubset(hot.columns):
        st.map(hot.rename(columns={"LAT": "lat", "LON": "lon"}), use_container_width=True)

    # Show a manageable slice
    show_cols = ["HUC10_NAME", "HUC12_NAME", "LAT", "LON"]
    for c in [ph_col, fe_col, so4_col, cond_col]:
        if c and c not in show_cols:
            show_cols.append(c)
    st.dataframe(hot[show_cols].head(1000))

# =============================== HUC summary ============================
with tab_huc:
    st.subheader("🌊 HUC Summary (Top 15 by Record Count)")
    huc_cols = [c for c in df_display.columns if "HUC10" in c or "HUC12" in c]
    if huc_cols:
        huc_counts = (
            df_display[huc_cols]
            .fillna("Unknown")
            .value_counts()
            .reset_index(name="Count")
            .head(15)
        )
        st.dataframe(huc_counts, use_container_width=True)
    else:
        st.info("No HUC fields found in dataset.")

# ============================== Correlations ============================
with tab_corr:
    st.subheader("🔬 Quick Correlation (Top analytes)")
    # Build a numeric frame with top_chem columns
    num_df = pd.DataFrame(index=gdf.index)
    for c in top_chem:
        num_df[c] = pd.to_numeric(gdf[c], errors="coerce")
    num_df = num_df.dropna(how="all")
    if num_df.shape[1] >= 2:
        corr = num_df.corr(numeric_only=True)
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(corr, cmap="RdBu_r", center=0, annot=True)
        st.pyplot(fig)
    else:
        st.info("Not enough populated analytes for a correlation matrix.")

st.caption("© 2025 OSMRE • Dashboard excludes inlet/outlet analytes by design.")