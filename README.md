# 🚕 NYC Yellow Taxi Big Data Analytics & Query Engine Benchmark

An end-to-end Big Data pipeline and interactive OLAP dashboard analyzing **39.5 Million NYC Yellow Taxi trips (Full Year 2024)**. Demonstrates modern high-performance data processing using **Polars**, **DuckDB**, **Streamlit**, and **Plotly**, with an in-depth performance benchmark against **Pandas**.

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![DuckDB](https://img.shields.io/badge/Engine-DuckDB%202.0-yellow)
![Polars](https://img.shields.io/badge/Engine-Polars%202.1-orange)
![Streamlit](https://img.shields.io/badge/UI-Streamlit-red)
![Data](https://img.shields.io/badge/Records-39.5M-green)

---

## 🏛️ Pipeline Architecture

This project implements a **Medallion Architecture**:

1. **🥉 Bronze Layer (`data/bronze_raw/`)**:
   - Ingests all 12 monthly Parquet files for 2024 (Jan–Dec) from CloudFront (~650 MB) + NYC Taxi Zone lookup CSV.
2. **🥈 Silver Layer (`data/silver_partitioned/`)**:
   - Cleaned via **Polars** with lazy streaming evaluation (`scan_parquet` + `sink_parquet`).
   - Filters out invalid rows (`trip_distance <= 0.1`, `fare_amount <= 0`), calculates `duration_mins` and `pickup_hour`, and removes GPS speed outliers (`1 <= speed <= 65 mph`).
   - Generates 39,483,380 cleaned records into `cleaned_trips.parquet`.
3. **🥇 Gold Layer (`data/gold_warehouse.duckdb`)**:
   - High-speed columnar analytics warehouse powered by **DuckDB**.
   - Contains pre-aggregated OLAP tables (`borough_summary`, `hourly_summary`, and `benchmark_results`) in a compact ~1.26 MB database file.

---

## ⚡ Performance Benchmark (39.5 Million Rows)

Benchmark measuring a multi-column join and aggregation query across all 39.5M rows:

| Engine | Execution Time | Speedup vs. Pandas | Peak RAM (MB) | RAM Reduction |
| :--- | :--- | :--- | :--- | :--- |
| 🦆 **DuckDB** | **7.29 s** | **18.85x** | **172.5 MB** | **-89.5%** |
| 🦀 **Polars** | **10.57 s** | **13.00x** | **233.7 MB** | **-85.8%** |
| 🐼 **Pandas** | **137.45 s** | 1.00x (baseline) | **1,648.9 MB** (~1.65 GB) | 0.0% |

- **DuckDB** demonstrated the fastest execution and lowest memory footprint via vectorized execution and column projection pushdown.
- **Polars** utilized native multi-threaded streaming to complete the query in ~10 seconds with under 235 MB RAM.
- **Pandas** suffered from large in-memory DataFrame duplications during merge and groupby operations.

---

## 📊 Analytics Summary (Full Year 2024)

- **Total Trips**: 39,483,380
- **Total Revenue**: $1,126,455,500 (~$1.13 Billion USD)
- **Average Fare**: $20.21 per trip
- **Peak Hour**: 18:00 (6:00 PM) with 2,801,780 trips
- **Borough Distribution**: Manhattan accounted for 88.9% of all trips ($839.9M revenue), followed by Queens ($265.1M revenue).

---

## 🚀 Getting Started

### 1. Clone & Set Up Environment
```bash
git clone https://github.com/<your-username>/nyc-bigdata-project.git
cd nyc-bigdata-project

python -m venv .venv
source .venv/bin/activate   # On Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Launch the Streamlit Dashboard
The repository includes the pre-computed `gold_warehouse.duckdb` warehouse, so the dashboard runs immediately without downloading raw data:
```bash
streamlit run app.py
```
Open [http://localhost:8501](http://localhost:8501) in your browser.

https://nyc-taxi-bigdata-analyticsgit-ve5g2wsisgn5jd736egtcb.streamlit.app/

### 3. Re-running the Full Pipeline (Optional)
To re-download raw data and rebuild the entire warehouse:
```bash
python src/01_etl_polars.py     # Download 12 months & run Polars ETL
python src/02_olap_duckdb.py     # Run DuckDB OLAP aggregations
python src/03_benchmark.py       # Run Pandas vs Polars vs DuckDB benchmark
```

---

## 📁 Repository Structure
```
nyc-bigdata-project/
├── app.py                     # Streamlit & Plotly interactive dashboard
├── data/
│   └── gold_warehouse.duckdb  # Lightweight analytics database (~1.2 MB)
├── src/
│   ├── 01_etl_polars.py       # 12-month data download & Polars lazy streaming ETL
│   ├── 02_olap_duckdb.py      # DuckDB OLAP SQL aggregations
│   └── 03_benchmark.py        # Isolated multi-process performance benchmark
├── .gitignore                 # Excludes heavy raw & silver data layers
├── requirements.txt           # Python package dependencies
└── README.md
```
