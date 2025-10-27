# AMD Water Quality Pipeline

A complete, reproducible Python workflow for analyzing **acid mine drainage (AMD)** and **water-quality** impacts across Pennsylvania's abandoned mine lands.

This project integrates **EPA Water Quality Portal (WQP)** data with **AMLIS** and mine-map datasets to evaluate watershed health, proximity to legacy mines, and post-reclamation recovery trends.

---

## 🧱 Features
- **Automated data pull** from WQP by HUC or bounding box  
- **Standardized chemistry cleaning** (Fe, Mn, SO₄, pH, etc.)  
- **Mine proximity analysis** using AMLIS + PASDA spatial data  
- **Feature engineering** for exceedances and AMD indicators  
- **Watershed-level summaries** and anomaly detection  
- **Streamlit dashboard** for interactive exploration  
- **Outputs** as CSV, GeoPackage, and visual plots  

---

## 🚀 Quick Start

```bash
git clone https://github.com/ashleyreagan/AMD_Water_Quality_Pipeline.git
cd AMD_Water_Quality_Pipeline
pip install -r requirements.txt
python src/00_download_clean_wqx.py --state PA --start 2010-01-01 --end 2025-01-01
```

**Outputs:**
- `data/outputs/wqx_pa_sites_merged_AMD_features.csv`  
- `data/outputs/wqx_pa_sites_merged_AMDsubset_param_summary.csv`  
- `data/outputs/wqp_mine_summary_stats.csv`  

---

## 🗺️ Example Workflow

```bash
# 1. Download WQP data
python src/00_download_clean_wqx.py --huc8 05050003

# 2. Clean & standardize parameters
python src/amd_cleaner.py

# 3. Join with AMLIS mine data
python src/mine_join_pipeline.py

# 4. Generate watershed stats
python src/watershed_stats.py

# 5. Visualize or launch dashboard
python src/pa11_visualize.py
streamlit run src/pa13_dashboard.py
```

---

## 🧠 Project Structure

```
AMD_Water_Quality_Pipeline/
├─ src/
│   ├─ 00_download_clean_wqx.py
│   ├─ amd_cleaner.py
│   ├─ amd_feature_engineer.py
│   ├─ watershed_stats.py
│   ├─ pa11_visualize.py
│   ├─ pa13_dashboard.py
│   └─ mine_join_pipeline.py
├─ data/
│   ├─ example_inputs/
│   ├─ outputs/
│   └─ logs/
├─ docs/
│   ├─ flow_diagram.png
│   ├─ data_dictionary.md
│   └─ citation.md
└─ requirements.txt
```

---

## 📊 Data Sources
- **EPA Water Quality Portal (WQP)** — https://www.waterqualitydata.us  
- **AMLIS** — Abandoned Mine Land Inventory System (OSMRE)  
- **PASDA** — Pennsylvania Spatial Data Access  
- **USGS NHDPlus / HUC datasets**  

---

## 🧩 Dependencies
See `requirements.txt` for full environment setup.

---

## 📄 License
MIT License © 2025 Ashley Mitchell

---

## 💬 Citation
If you use this workflow in research or restoration reporting, please cite:  
> Mitchell, A. (2025). *AMD Water Quality Pipeline: Automated Integration of WQP and AMLIS for Watershed Restoration Analytics.* GitHub. https://github.com/ashleyreagan/AMD_Water_Quality_Pipeline
