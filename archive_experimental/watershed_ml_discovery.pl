#!/usr/bin/env python3
"""
watershed_ml_discovery.py
Unsupervised clustering of watershed chemistry data using PCA + KMeans.
Author: Ashley Reagan Mitchell
"""

import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import plotly.express as px

# =========================================================
# Paths
# =========================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(base_dir, "data_outputs")

# Automatically find all *_latest.csv chemistry files
chem_files = [f for f in os.listdir(data_dir) if f.endswith("_latest.csv")]

if not chem_files:
    raise FileNotFoundError("No *_latest.csv chemistry files found in data_outputs/")

print(f"🧩 Found {len(chem_files)} chemistry files")
for f in chem_files:
    print("   ├─", f)

# =========================================================
# Merge Chemistry Files
# =========================================================
dfs = []
for f in chem_files:
    path = os.path.join(data_dir, f)
    df = pd.read_csv(path)
    if "MonitoringLocationIdentifier" in df.columns and "ResultMeasureValue" in df.columns:
        df = df.rename(columns={"MonitoringLocationIdentifier": "SiteID"})
        df["CharacteristicName"] = f.replace("_latest.csv", "").title()
        dfs.append(df[["SiteID", "CharacteristicName", "ResultMeasureValue", "LatitudeMeasure", "LongitudeMeasure", "HUCEightDigitCode", "HUCTwelveDigitCode"]])

if not dfs:
    raise RuntimeError("No valid data found in chemistry files.")

data = pd.concat(dfs, ignore_index=True)

# =========================================================
# Clean & Aggregate
# =========================================================
print("🧹 Cleaning and aggregating data...")
data = data.dropna(subset=["ResultMeasureValue"])
data["ResultMeasureValue"] = pd.to_numeric(data["ResultMeasureValue"], errors="coerce")
data = data.dropna(subset=["ResultMeasureValue"])
data = data.rename(columns={"HUCTwelveDigitCode": "HUC12"})

# Average by HUC12 and characteristic
chem_pivot = (
    data.groupby(["HUC12", "CharacteristicName"])["ResultMeasureValue"]
    .mean()
    .unstack()
    .reset_index()
)

chem_pivot = chem_pivot.dropna(axis=0, how="any")  # drop incomplete rows

# =========================================================
# Normalize
# =========================================================
features = chem_pivot.drop(columns=["HUC12"])
scaler = StandardScaler()
X_scaled = scaler.fit_transform(features)

# =========================================================
# PCA
# =========================================================
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)

explained_var = np.sum(pca.explained_variance_ratio_) * 100
print(f"🔍 PCA completed (2 components explain {explained_var:.2f}% variance)")

# =========================================================
# KMeans Clustering
# =========================================================
k = 4  # default clusters
model = KMeans(n_clusters=k, random_state=42, n_init=10)
labels = model.fit_predict(X_pca)
sil_score = silhouette_score(X_pca, labels)
print(f"🤖 KMeans clustering done (k={k}, silhouette={sil_score:.3f})")

chem_pivot["Cluster"] = labels
chem_pivot["PC1"] = X_pca[:, 0]
chem_pivot["PC2"] = X_pca[:, 1]

# =========================================================
# Save Outputs
# =========================================================
os.makedirs(data_dir, exist_ok=True)
chem_pivot.to_csv(os.path.join(data_dir, "watershed_clusters.csv"), index=False)

cluster_summary = chem_pivot.groupby("Cluster").mean(numeric_only=True)
cluster_summary.to_csv(os.path.join(data_dir, "cluster_summary.csv"))

# =========================================================
# Plot
# =========================================================
fig = px.scatter(
    chem_pivot,
    x="PC1",
    y="PC2",
    color=chem_pivot["Cluster"].astype(str),
    hover_data=["HUC12"],
    title="Watershed Chemistry Clusters (PCA + KMeans)",
    color_discrete_sequence=px.colors.qualitative.Bold
)
fig.write_html(os.path.join(data_dir, "cluster_scatter.html"))

# =========================================================
# Done
# =========================================================
print(f"""
✅ Saved results:
   ├─ watershed_clusters.csv
   ├─ cluster_summary.csv
   ├─ cluster_scatter.html
📊 PCA variance explained: {explained_var:.2f}%
📈 Silhouette score: {sil_score:.3f}
""")