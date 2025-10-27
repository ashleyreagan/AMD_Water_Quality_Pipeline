# ============================================================
# classify_chemical.py
# Predicts watershed cluster from new chemistry samples
# ============================================================

import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import MiniBatchKMeans

# ------------------------------------------------------------
# Load trained artifacts
# ------------------------------------------------------------
MODEL_PATH = "data_outputs/kmeans_model.pkl"
PCA_PATH = "data_outputs/pca_model.pkl"
SUMMARY_PATH = "data_outputs/watershed_cluster_summary.csv"

kmeans = joblib.load(MODEL_PATH)
pca = joblib.load(PCA_PATH)
summary = pd.read_csv(SUMMARY_PATH)

# ------------------------------------------------------------
# Define function
# ------------------------------------------------------------
def classify_sample(sample_dict):
    """
    sample_dict example:
    {"Iron": 3.2, "Conductivity": 420, "Temperature": 11.5, "pH": 6.4}
    """
    sample = pd.DataFrame([sample_dict])
    # Ensure numeric features only
    X = sample.select_dtypes("number").fillna(0)
    # Match PCA input
    X_pca = pca.transform(X)
    cluster = int(kmeans.predict(X_pca)[0])

    cluster_row = summary[summary["Cluster"] == cluster]
    print(f"Predicted cluster: {cluster}")
    if not cluster_row.empty:
        print(cluster_row[["Iron_mean", "Cond_mean", "Temp_mean", "Sickness_Index"]])
    else:
        print("No summary info available for this cluster.")
    return cluster

# ------------------------------------------------------------
# Example usage
# ------------------------------------------------------------
if __name__ == "__main__":
    sample = {
        "Iron": 4.8,
        "Conductivity": 560,
        "Temperature": 10.5,
        "pH": 6.3
    }
    classify_sample(sample)