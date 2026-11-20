import sys
import time
from pathlib import Path
import duckdb

# -----------------------------------------------------------------------------
# Configuration and Paths
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SILVER_PARQUET = DATA_DIR / "silver_partitioned" / "cleaned_trips.parquet"
BRONZE_ZONE_CSV = DATA_DIR / "bronze_raw" / "taxi_zone_lookup.csv"
GOLD_DB_PATH = DATA_DIR / "gold_warehouse.duckdb"


def run_olap_duckdb() -> None:
    print("=" * 70)
    print("RUNNING DUCKDB OLAP AGGREGATIONS")
    print("=" * 70)

    if not SILVER_PARQUET.exists():
        raise FileNotFoundError(f"Cleaned trips parquet not found at: {SILVER_PARQUET}")
    if not BRONZE_ZONE_CSV.exists():
        raise FileNotFoundError(f"Taxi zone lookup CSV not found at: {BRONZE_ZONE_CSV}")

    # Connect to persistent DuckDB database in data/gold_warehouse.duckdb
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(GOLD_DB_PATH))
    print(f"[CONNECTED] Connected to DuckDB warehouse: {GOLD_DB_PATH}")

    # Format file paths for DuckDB SQL (use forward slashes)
    parquet_path_sql = str(SILVER_PARQUET).replace("\\", "/")
    csv_path_sql = str(BRONZE_ZONE_CSV).replace("\\", "/")

    # -------------------------------------------------------------------------
    # (a) Total trips, average fare, and total revenue by pickup NYC Borough
    # -------------------------------------------------------------------------
    print("\n[QUERY A] Computing total trips, avg fare, and revenue by NYC Borough...")
    t0 = time.perf_counter()
    borough_query = f"""
        CREATE OR REPLACE TABLE borough_summary AS
        SELECT 
            COALESCE(z.Borough, 'Unknown') AS borough,
            COUNT(*) AS total_trips,
            ROUND(AVG(t.fare_amount), 2) AS avg_fare,
            ROUND(SUM(t.total_amount), 2) AS total_revenue
        FROM '{parquet_path_sql}' t
        LEFT JOIN '{csv_path_sql}' z
            ON t.PULocationID = z.LocationID
        WHERE t.duration_mins > 0 
          AND (t.trip_distance / (t.duration_mins / 60.0)) BETWEEN 1.0 AND 65.0
        GROUP BY COALESCE(z.Borough, 'Unknown')
        ORDER BY total_trips DESC;
    """
    con.execute(borough_query)
    borough_time = time.perf_counter() - t0
    print(f"  -> Created 'borough_summary' in {borough_time:.3f}s")

    borough_df = con.execute("SELECT * FROM borough_summary").df()
    print("\n--- Summary by Pickup Borough ---")
    print(borough_df.to_string(index=False))

    # -------------------------------------------------------------------------
    # (b) Hourly trip volume and average speed by hour of day
    # -------------------------------------------------------------------------
    print("\n[QUERY B] Computing hourly trip volume and average speed by hour of day (speed 1-65 mph)...")
    t0 = time.perf_counter()
    hourly_query = f"""
        CREATE OR REPLACE TABLE hourly_summary AS
        SELECT 
            t.pickup_hour,
            COUNT(*) AS trip_volume,
            ROUND(AVG(t.trip_distance / (t.duration_mins / 60.0)), 2) AS avg_speed_mph
        FROM '{parquet_path_sql}' t
        WHERE t.duration_mins > 0 
          AND (t.trip_distance / (t.duration_mins / 60.0)) BETWEEN 1.0 AND 65.0
        GROUP BY t.pickup_hour
        ORDER BY t.pickup_hour ASC;
    """
    con.execute(hourly_query)
    hourly_time = time.perf_counter() - t0
    print(f"  -> Created 'hourly_summary' in {hourly_time:.3f}s")

    hourly_df = con.execute("SELECT * FROM hourly_summary").df()
    print("\n--- Summary by Hour of Day ---")
    print(hourly_df.to_string(index=False))

    # -------------------------------------------------------------------------
    # Verification & Metadata
    # -------------------------------------------------------------------------
    tables = con.execute("SHOW TABLES").df()
    print("\n" + "=" * 70)
    print("GOLD WAREHOUSE TABLES")
    print("=" * 70)
    print(tables.to_string(index=False))
    print(f"\n[DONE] Successfully updated {GOLD_DB_PATH}")
    con.close()


if __name__ == "__main__":
    try:
        run_olap_duckdb()
    except Exception as e:
        print(f"\n[ERROR] DuckDB OLAP failed: {e}", file=sys.stderr)
        sys.exit(1)
