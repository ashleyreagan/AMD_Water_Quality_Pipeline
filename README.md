# AMD Water Quality Pipeline

The **AMD Water Quality Pipeline** is a reproducible, open-source Python framework for analyzing **acid mine drainage (AMD)** and **water-quality dynamics** across Pennsylvania’s abandoned mine lands (AML).  
It integrates **EPA Water Quality Portal (WQP)** observations with **OSMRE AMLIS** data and **state mine-map inventories** to quantify watershed impacts, evaluate restoration outcomes, and support data-driven decision making.

---

## 🌎 Project Overview

Mining legacies across Appalachia have left persistent acid mine drainage (AMD) impairments in surface and groundwater.  
This pipeline automates the ingestion, cleaning, and spatial analysis of publicly available water-quality datasets to evaluate trends in parameters such as pH, iron, manganese, sulfate, and alkalinity.

The workflow is modular and can be adapted for other states or data sources.  
It supports integration into **GIS**, **machine learning**, and **dashboard environments** for rapid exploration of restoration progress or risk detection.

---

## 🧱 Features

- 🔄 **Automated Data Retrieval:** Downloads WQP data by HUC, bounding box, or state.
- 🧪 **Chemistry Cleaning:** Standardizes analyte names, units, and detection flags.
- 🗺️ **Mine Proximity Analysis:** Links monitoring stations to AMLIS sites and PASDA mine features.
- 📈 **Feature Engineering:** Calculates exceedances, summary statistics, and AMD indicator scores.
- 🧠 **Watershed-Level Analytics:** Aggregates results to HUC-8 or HUC-12 units with hydrologic joins.
- 🎛️ **Visualization and Dashboard:** Generates static charts or interactive Streamlit dashboards.
- 📦 **Reproducible Outputs:** Exports clean tables and GeoPackages for use in ArcGIS/QGIS or further modeling.

---

## 🧬 Data Flow

WQP API  →  AMD Cleaner  →  Standardizer  →  Mine Join  →  Feature Engineering  →  Watershed Stats  →  Dashboard

Each stage produces logged outputs and intermediate CSV/GeoPackage layers, stored in `/data/outputs/` with corresponding metadata.

---

## 🚀 Quick Start
```
git clone https://github.com/ashleyreagan/AMD_Water_Quality_Pipeline.git
cd AMD_Water_Quality_Pipeline
pip install -r requirements.txt
python src/00_download_clean_wqx.py --state PA --start 2010-01-01 --end 2025-01-01

**Example Outputs:**
- data/outputs/wqx_pa_sites_merged_AMD_features.csv  
- data/outputs/wqx_pa_sites_merged_AMDsubset_param_summary.csv  
- data/outputs/wqp_mine_summary_stats.csv  
```
---

## 🗺️ Example Workflow

# 1. Download WQP data
```python src/00_download_clean_wqx.py --huc8 05050003```

# 2. Clean & standardize parameters
```python src/amd_cleaner.py```

# 3. Join with AMLIS and mine-map data
```python src/mine_join_pipeline.py```

# 4. Generate watershed-level summaries
```python src/watershed_stats.py```

# 5. Visualize results or launch dashboard
```python src/pa11_visualize.py```
```streamlit run src/pa13_dashboard.py```

---

## 🧠 Repository Structure
```
AMD_Water_Quality_Pipeline/
├─ src/
│   ├─ 00_download_clean_wqx.py         # WQP data ingestion
│   ├─ amd_cleaner.py                   # Cleaning and QA/QC
│   ├─ chemistry_standardize.py         # Standard analyte mapping
│   ├─ cleanup_unify.py                 # Merges cleaned datasets
│   ├─ amd_feature_engineer.py          # Adds derived variables
│   ├─ watershed_stats.py               # HUC-level summaries
│   ├─ pa11_visualize.py                # Chart generation
│   ├─ pa13_dashboard.py                # Streamlit interface
│   └─ mine_join_pipeline.py            # Spatial joins with AMLIS/PASDA
├─ data/
│   ├─ example_inputs/                  # Sample data for testing
│   ├─ outputs/                         # Final CSV/GeoPackage results
│   └─ logs/                            # Run-time logs and QA reports
├─ docs/
│   ├─ flow_diagram.png
│   ├─ data_dictionary.md
│   └─ citation.md
├─ archive_experimental/                # Legacy and prototype scripts
├─ requirements.txt
├─ .gitignore
└─ LICENSE
```
---

## 📊 Data Sources

| Dataset | Provider | Description |
|----------|-----------|-------------|
| **WQP (Water Quality Portal)** | EPA / USGS | National repository of chemistry data from state and federal monitoring networks. |
| **AMLIS** | OSMRE | Federal inventory of abandoned mine features and reclamation projects. |
| **PASDA Mine Maps** | Pennsylvania Spatial Data Access | Spatial footprints of legacy mining operations. |
| **NHDPlus / HUC Boundaries** | USGS | Hydrologic unit data used for watershed aggregation. |

---

## 🧩 Dependencies

All dependencies are listed in requirements.txt.  
Install with:

pip install -r requirements.txt

Key packages include:
- pandas, geopandas, rioxarray – Data wrangling and geospatial handling  
- scikit-learn, xgboost – Machine learning and feature analysis  
- streamlit, matplotlib, seaborn – Visualization and dashboards  
- requests, tqdm – API access and progress tracking  

---

## 🔬 Future Development

- 🌐 Multi-state expansion for the Appalachian coal region  
- 🤖 Integration with MineAI for predictive AMD risk mapping  
- 💧 Automated anomaly detection for pH and metal concentration spikes  
- 📚 Streamlit app publishing for public-facing watershed reports  
- 🗂️ Enhanced data provenance tracking and reproducible logs  

---

## 💡 Contributing

Pull requests are welcome!  
Please open an issue first to discuss proposed changes or new features.

When contributing:
1. Create a feature branch (git checkout -b feature-name)
2. Add or update relevant documentation
3. Run tests and verify output structure

---

## 📄 License

MIT License © 2025 Ashley Mitchell  
You are free to reuse and modify this work with attribution.

---

## 💬 Citation

If you use this workflow in research or restoration reporting, please cite:

Mitchell, A. (2025). *AMD Water Quality Pipeline: Automated Integration of WQP and AMLIS for Watershed Restoration Analytics.* GitHub. https://github.com/ashleyreagan/AMD_Water_Quality_Pipeline

---

## 🛰️ Acknowledgments

This work builds on the mission of the Office of Surface Mining Reclamation and Enforcement (OSMRE) and the broader Appalachian restoration community.  
Special thanks to the open-data initiatives of EPA, USGS, PASDA, and state environmental agencies whose datasets make this analysis possible.
