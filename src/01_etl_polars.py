import concurrent.futures
import sys
import time
from pathlib import Path
import requests
import polars as pl

# -----------------------------------------------------------------------------
# Configuration and Paths
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze_raw"
SILVER_DIR = DATA_DIR / "silver_partitioned"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{m:02d}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

ZONE_RAW_PATH = BRONZE_DIR / "taxi_zone_lookup.csv"
CLEANED_PARQUET_PATH = SILVER_DIR / "cleaned_trips.parquet"

MONTHS = list(range(1, 13))
PARQUET_FILES = [
    (m, BASE_URL.format(m=m), BRONZE_DIR / f"yellow_tripdata_2024-{m:02d}.parquet")
    for m in MONTHS
]


def download_file(url: str, dest_path: Path, max_retries: int = 5) -> Path:
    """Download a file from url to dest_path with retries if it doesn't already exist."""
    if dest_path.exists() and dest_path.stat().st_size > 1024 * 1024:
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"[SKIP] '{dest_path.name}' already exists ({size_mb:.2f} MB).")
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = dest_path.with_suffix(dest_path.suffix + ".download")

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[DOWNLOADING] '{dest_path.name}' (Attempt {attempt}/{max_retries})...")
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            start_time = time.time()
            with requests.get(url, headers=headers, stream=True, timeout=240) as resp:
                resp.raise_for_status()
                with open(temp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            temp_path.replace(dest_path)
            elapsed = time.time() - start_time
            size_mb = dest_path.stat().st_size / (1024 * 1024)
            print(f"[DOWNLOADED] Finished '{dest_path.name}' ({size_mb:.2f} MB) in {elapsed:.1f}s.")
            return dest_path
        except Exception as e:
            print(f"[RETRY] Failed attempt {attempt} for '{dest_path.name}': {e}")
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            if attempt == max_retries:
                raise
            time.sleep(3 * attempt)


def download_all_data() -> list[Path]:
    """Ensure zone lookup CSV and all 12 monthly taxi parquet files are downloaded."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Ensure zone CSV
    if not ZONE_RAW_PATH.exists() or ZONE_RAW_PATH.stat().st_size == 0:
        download_file(ZONE_URL, ZONE_RAW_PATH)
    else:
        print(f"[SKIP] '{ZONE_RAW_PATH.name}' already exists.")

    print(f"\n[DOWNLOAD] Checking and downloading 2024 monthly files (1 to 12)...")
    paths = []
    # Using 2 workers to avoid connection resets from CloudFront
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_file = {
            executor.submit(download_file, url, path): path
            for _, url, path in PARQUET_FILES
        }
        for future in concurrent.futures.as_completed(future_to_file):
            path = future.result()
            paths.append(path)

    paths.sort()
    return paths


def run_etl() -> None:
    """Execute lazy ETL pipeline across all 12 months using Polars scan_parquet."""
    print("=" * 65)
    print("STARTING 12-MONTH POLARS ETL PIPELINE (2024-01 to 2024-12)")
    print("=" * 65)

    # 1. Download raw files
    paths = download_all_data()

    # 2. Inspect raw row count lazily across all 12 files
    start_time = time.time()
    print(f"\n[SCAN] Reading {len(paths)} raw parquet files lazily...")
    raw_lf = pl.scan_parquet(paths)

    print("[INFO] Computing total raw row count across all 12 months...")
    raw_row_count = raw_lf.select(pl.len()).collect().item()
    print(f"[INFO] Total raw rows (12 months): {raw_row_count:,}")

    # 3. Apply transformations lazily:
    #    - Filter invalid rows: trip_distance <= 0.1, fare_amount <= 0
    #    - Calculate duration_mins ((dropoff - pickup) in minutes)
    #    - Calculate pickup_hour
    #    - Filter out GPS speed outliers: speed must be between 1 and 65 mph
    print("\n[TRANSFORM] Applying filters (trip_distance > 0.1, fare_amount > 0)...")
    print("[TRANSFORM] Adding derived columns: duration_mins, pickup_hour...")
    print("[TRANSFORM] Filtering GPS speed outliers (speed between 1.0 and 65.0 mph)...")

    cleaned_lf = (
        raw_lf.filter(
            (pl.col("trip_distance") > 0.1) & (pl.col("fare_amount") > 0)
        )
        .with_columns(
            (
                (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_seconds() / 60.0
            ).round(2).alias("duration_mins"),
            pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
        )
        .filter(
            (pl.col("duration_mins") > 0)
            & (
                (pl.col("trip_distance") / (pl.col("duration_mins") / 60.0)).is_between(1.0, 65.0)
            )
        )
    )

    # 4. Save cleaned dataset to silver layer using streaming sink
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n[SINK] Streaming cleaned dataset to {CLEANED_PARQUET_PATH}...")
    t_sink = time.time()
    cleaned_lf.sink_parquet(CLEANED_PARQUET_PATH)
    sink_duration = time.time() - t_sink
    elapsed_total = time.time() - start_time

    output_size_mb = CLEANED_PARQUET_PATH.stat().st_size / (1024 * 1024)

    # 5. Summary verification
    print("[VALIDATE] Computing summary metrics on cleaned dataset...")
    summary = pl.scan_parquet(CLEANED_PARQUET_PATH).select(
        pl.len().alias("cleaned_rows"),
        pl.col("trip_distance").min().alias("min_dist"),
        pl.col("trip_distance").max().alias("max_dist"),
        pl.col("fare_amount").min().alias("min_fare"),
        pl.col("fare_amount").max().alias("max_fare"),
        pl.col("duration_mins").mean().round(2).alias("avg_duration_mins"),
    ).collect()

    cleaned_rows = summary["cleaned_rows"][0]
    removed_rows = raw_row_count - cleaned_rows
    retention_pct = (cleaned_rows / raw_row_count) * 100

    print("\n" + "=" * 65)
    print("12-MONTH ETL SUMMARY & VALIDATION")
    print("=" * 65)
    print(f"Raw rows (12 months): {raw_row_count:,}")
    print(f"Cleaned rows:         {cleaned_rows:,} ({retention_pct:.2f}% retained)")
    print(f"Removed rows:         {removed_rows:,} ({100 - retention_pct:.2f}% filtered)")
    print(f"Cleaned file size:    {output_size_mb:.2f} MB")
    print(f"Streaming sink time:  {sink_duration:.2f}s (Total ETL: {elapsed_total:.2f}s)")
    print("\nSample Metrics from Cleaned Dataset:")
    print(summary)
    print(f"\n[DONE] Successfully saved to {CLEANED_PARQUET_PATH}")


if __name__ == "__main__":
    try:
        run_etl()
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)
