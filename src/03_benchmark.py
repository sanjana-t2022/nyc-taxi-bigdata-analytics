import multiprocessing as mp
import os
import sys
import time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import polars as pl
import psutil

# -----------------------------------------------------------------------------
# Configuration and Paths
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SILVER_PARQUET = DATA_DIR / "silver_partitioned" / "cleaned_trips.parquet"
BRONZE_ZONE_CSV = DATA_DIR / "bronze_raw" / "taxi_zone_lookup.csv"
GOLD_DB_PATH = DATA_DIR / "gold_warehouse.duckdb"


# -----------------------------------------------------------------------------
# Isolated Worker Functions for Precise Resource Measurement
# -----------------------------------------------------------------------------
def run_pandas_worker(parquet_path: str, csv_path: str, queue: mp.Queue) -> None:
    """Run borough aggregation query using Pandas in an isolated process."""
    proc = psutil.Process(os.getpid())
    t0 = time.perf_counter()

    trips = pd.read_parquet(parquet_path, columns=["PULocationID", "fare_amount", "total_amount"])
    zones = pd.read_csv(csv_path, keep_default_na=False)

    merged = trips.merge(zones, left_on="PULocationID", right_on="LocationID", how="left")
    merged["Borough"] = merged["Borough"].replace("", "Unknown").fillna("Unknown")

    res = (
        merged.groupby("Borough", as_index=False)
        .agg(
            total_trips=("fare_amount", "count"),
            avg_fare=("fare_amount", "mean"),
            total_revenue=("total_amount", "sum"),
        )
        .sort_values("total_trips", ascending=False)
    )

    elapsed = time.perf_counter() - t0
    peak_ram_mb = proc.memory_info().peak_wset / (1024 * 1024)
    queue.put(("Pandas", elapsed, peak_ram_mb, len(res)))


def run_polars_worker(parquet_path: str, csv_path: str, queue: mp.Queue) -> None:
    """Run borough aggregation query using Polars (lazy scan) in an isolated process."""
    proc = psutil.Process(os.getpid())
    t0 = time.perf_counter()

    trips = pl.scan_parquet(parquet_path).select(["PULocationID", "fare_amount", "total_amount"])
    zones = pl.scan_csv(csv_path)

    res = (
        trips.join(zones, left_on="PULocationID", right_on="LocationID", how="left")
        .with_columns(pl.col("Borough").fill_null("Unknown"))
        .group_by("Borough")
        .agg([
            pl.len().alias("total_trips"),
            pl.col("fare_amount").mean().round(2).alias("avg_fare"),
            pl.col("total_amount").sum().round(2).alias("total_revenue"),
        ])
        .sort("total_trips", descending=True)
        .collect()
    )

    elapsed = time.perf_counter() - t0
    peak_ram_mb = proc.memory_info().peak_wset / (1024 * 1024)
    queue.put(("Polars", elapsed, peak_ram_mb, len(res)))


def run_duckdb_worker(parquet_path: str, csv_path: str, queue: mp.Queue) -> None:
    """Run borough aggregation query using DuckDB in an isolated process."""
    proc = psutil.Process(os.getpid())
    t0 = time.perf_counter()

    con = duckdb.connect()
    parquet_sql = parquet_path.replace("\\", "/")
    csv_sql = csv_path.replace("\\", "/")

    res = con.execute(f"""
        SELECT 
            COALESCE(z.Borough, 'Unknown') AS Borough,
            COUNT(*) AS total_trips,
            ROUND(AVG(t.fare_amount), 2) AS avg_fare,
            ROUND(SUM(t.total_amount), 2) AS total_revenue
        FROM '{parquet_sql}' t
        LEFT JOIN '{csv_sql}' z
            ON t.PULocationID = z.LocationID
        GROUP BY COALESCE(z.Borough, 'Unknown')
        ORDER BY total_trips DESC;
    """).df()

    elapsed = time.perf_counter() - t0
    peak_ram_mb = proc.memory_info().peak_wset / (1024 * 1024)
    queue.put(("DuckDB", elapsed, peak_ram_mb, len(res)))


# -----------------------------------------------------------------------------
# Main Benchmark Runner
# -----------------------------------------------------------------------------
def run_benchmark() -> None:
    print("=" * 75)
    print("BENCHMARK: Pandas vs. Polars vs. DuckDB (Borough Aggregation)")
    print("=" * 75)

    if not SILVER_PARQUET.exists():
        raise FileNotFoundError(f"Cleaned trips parquet not found at: {SILVER_PARQUET}")
    if not BRONZE_ZONE_CSV.exists():
        raise FileNotFoundError(f"Taxi zone lookup CSV not found at: {BRONZE_ZONE_CSV}")

    benchmarks = [
        ("Pandas", run_pandas_worker),
        ("Polars", run_polars_worker),
        ("DuckDB", run_duckdb_worker),
    ]

    results = []
    parquet_str = str(SILVER_PARQUET)
    csv_str = str(BRONZE_ZONE_CSV)

    q = mp.Queue()
    for engine_name, target_fn in benchmarks:
        print(f"\n[RUNNING] Executing {engine_name} benchmark in isolated process...")
        p = mp.Process(target=target_fn, args=(parquet_str, csv_str, q))
        p.start()
        p.join()

        engine, elapsed, peak_ram_mb, rows = q.get()
        print(f"  -> {engine}: {elapsed:.3f} seconds | Peak RAM: {peak_ram_mb:.1f} MB (Rows: {rows})")
        results.append({
            "engine": engine,
            "execution_time_seconds": round(elapsed, 4),
            "peak_ram_mb": round(peak_ram_mb, 2),
            "rows_returned": rows,
        })

    # Calculate speedup and memory efficiency relative to Pandas
    pandas_time = next(r["execution_time_seconds"] for r in results if r["engine"] == "Pandas")
    pandas_ram = next(r["peak_ram_mb"] for r in results if r["engine"] == "Pandas")

    now_ts = datetime.now()
    for r in results:
        r["speedup_factor"] = round(pandas_time / r["execution_time_seconds"], 2)
        r["ram_reduction_pct"] = round(((pandas_ram - r["peak_ram_mb"]) / pandas_ram) * 100, 1)
        r["benchmark_timestamp"] = now_ts

    results_df = pd.DataFrame(results)

    print("\n" + "=" * 75)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 75)
    display_df = results_df[["engine", "execution_time_seconds", "speedup_factor", "peak_ram_mb", "ram_reduction_pct"]]
    print(display_df.to_string(index=False))

    # Save benchmark results into gold_warehouse.duckdb
    print(f"\n[SAVE] Storing benchmark results into {GOLD_DB_PATH} (table: benchmark_results)...")
    con = duckdb.connect(str(GOLD_DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_results (
            engine VARCHAR,
            execution_time_seconds DOUBLE,
            peak_ram_mb DOUBLE,
            rows_returned BIGINT,
            speedup_factor DOUBLE,
            ram_reduction_pct DOUBLE,
            benchmark_timestamp TIMESTAMP
        );
    """)
    con.register("new_benchmark_df", results_df)
    con.execute("INSERT INTO benchmark_results SELECT * FROM new_benchmark_df")

    print("\n--- Recent Records in benchmark_results ---")
    stored_df = con.execute("SELECT * FROM benchmark_results ORDER BY benchmark_timestamp DESC, execution_time_seconds ASC LIMIT 6").df()
    print(stored_df.to_string(index=False))
    con.close()

    print(f"\n[DONE] Benchmark complete and saved to {GOLD_DB_PATH}")


if __name__ == "__main__":
    mp.freeze_support()
    try:
        run_benchmark()
    except Exception as e:
        print(f"\n[ERROR] Benchmark runner failed: {e}", file=sys.stderr)
        sys.exit(1)
