# ============================================================
# watershed_ml_discovery_v2.py
# Machine Learning Discovery for Watershed Chemistry Patterns
# ============================================================

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

# ------------------------------------------------------------
# 1️⃣ Load data
# ------------------------------------------------------------
input_path = Path("data_outputs/PA_wq_chemistry_clean.parquet")
df = pd.read_parquet(input_path)
print(f"✅ Loaded {len(df):,} records from {input_path.name}")

# ------------------------------------------------------------
# 2️⃣ Select feature columns
# ------------------------------------------------------------
chem_keywords = [
    "fe", "iron", "mn", "al", "ph", "temp", "cond", "so4", "sulf",
    "flow", "delta", "annualom", "cost", "maint"
]

chem_cols = [
    c for c in df.columns
    if any(k in c.lower() for k in chem_keywords)
    and pd.api.types.is_numeric_dtype(df[c])
]

if len(chem_cols) < 5:
    print(f"⚠️ Only {len(chem_cols)} chemistry fields found — expanding to all numeric columns.")
    chem_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

# Drop rows missing all these values
X = df[chem_cols].dropna(how="all")

if len(X) < 500:
    print(f"⚠️ Only {len(X)} usable rows — clustering may be unstable.")

print(f"🧪 Using {len(chem_cols)} numeric features across {len(X):,} samples.")
print(", ".join(chem_cols))

# ------------------------------------------------------------
# 3️⃣ Scale and reduce dimensionality
# ------------------------------------------------------------
X_scaled = StandardScaler().fit_transform(X.fillna(0))

pca = PCA(n_components=0.9, svd_solver="full")
X_pca = pca.fit_transform(X_scaled)

print(f"📉 PCA reduced features → {X_pca.shape[1]} components (90% variance)")

# ------------------------------------------------------------
# 4️⃣ Auto-select best cluster count (2–8)
# ------------------------------------------------------------
best_k, best_score = 2, -1
for k in range(2, 9):
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_pca)
    score = silhouette_score(X_pca, labels)
    if score > best_score:
        best_k, best_score = k, score

print(f"🏷️ Optimal cluster count: {best_k} (silhouette={best_score:.3f})")

kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=20)
clusters = kmeans.fit_predict(X_pca)
df["Cluster"] = np.nan
df.loc[X.index, "Cluster"] = clusters

# ------------------------------------------------------------
# 5️⃣ Cluster summary
# ------------------------------------------------------------
summary = df.groupby("Cluster")[chem_cols].mean().round(2)
summary["Count"] = df.groupby("Cluster").size()
summary = summary.reset_index()

print("\n📊 Cluster summary (first 10 columns):")
print(summary.iloc[:, :10])

# ------------------------------------------------------------
# 6️⃣ Save outputs
# ------------------------------------------------------------
out_dir = Path("data_outputs")
csv_path = out_dir / "PA_wq_clusters_v2.csv"
summary_path = out_dir / "PA_wq_cluster_summary_v2.csv"
jpg_path = out_dir / "PA_wq_cluster_plot_v2.jpg"
geo_path = out_dir / "PA_wq_clusters_v2.geojson"

df.to_csv(csv_path, index=False)
summary.to_csv(summary_path, index=False)

# ------------------------------------------------------------
# 7️⃣ Save GeoJSON if geometry present
# ------------------------------------------------------------
geo_cols = [c for c in df.columns if c.lower() in ("geometry", "geom", "wkt")]
if geo_cols:
    gdf = gpd.GeoDataFrame(df.dropna(subset=geo_cols), geometry=geo_cols[0], crs="EPSG:4326")
    gdf.to_file(geo_path, driver="GeoJSON")
    print(f"🗺️ GeoJSON saved → {geo_path}")
else:
    print("⚠️ No geometry column found — skipping GeoJSON export.")

# ------------------------------------------------------------
# 8️⃣ PCA scatter plot
# ------------------------------------------------------------
plt.figure(figsize=(8, 6))
scatter = plt.scatter(
    X_pca[:, 0],
    X_pca[:, 1],
    c=clusters,
    cmap="viridis",
    s=10,
    alpha=0.6
)
plt.colorbar(scatter, label="Cluster")
plt.title("PCA Cluster Visualization – Watershed Chemistry")
plt.xlabel("PCA 1")
plt.ylabel("PCA 2")
plt.tight_layout()
plt.savefig(jpg_path, dpi=300)
plt.close()
print(f"🖼️ PCA plot saved → {jpg_path}")

print("\n✅ watershed_ml_discovery_v2 complete.")
print(f"Outputs saved in: {out_dir.resolve()}")