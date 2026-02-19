"""
Docker version: loads parquet files from /app/parquet_output into Iceberg via dlt.
Config comes from environment variables set in docker-compose.
"""

import os
import resource
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import dlt


PARQUET_DIR = Path("/app/parquet_output")
LIMIT_FILES = int(os.environ.get("LIMIT_FILES", "0")) or None
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10000"))


def get_memory_mb() -> dict:
    """Get real memory usage including Arrow's C++ allocations."""
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    rss_mb = rusage.ru_maxrss / 1024  # Linux: kilobytes → MB
    arrow_mb = pa.total_allocated_bytes() / (1024 * 1024)
    return {"rss_mb": rss_mb, "arrow_mb": arrow_mb}


def log_memory(label: str) -> None:
    mem = get_memory_mb()
    print(f"  [{label}] RSS: {mem['rss_mb']:.0f} MB | Arrow pool: {mem['arrow_mb']:.0f} MB")


def parquet_resource(parquet_dir: str, limit_files: int | None = None, batch_size: int = 10_000):
    parquet_files = sorted(Path(parquet_dir).glob("*.parquet"))
    if limit_files:
        parquet_files = parquet_files[:limit_files]

    total_mb = sum(f.stat().st_size for f in parquet_files) / (1024**2)
    print(f"Loading {len(parquet_files)} files ({total_mb:.0f} MB) from {parquet_dir}")

    dataset = ds.dataset([str(f) for f in parquet_files], format="parquet")
    for batch in dataset.to_batches(batch_size=batch_size):
        yield batch


def main():
    if not PARQUET_DIR.exists() or not list(PARQUET_DIR.glob("*.parquet")):
        print(f"No parquet files found at {PARQUET_DIR}")
        print("Mount your parquet_output directory as a volume.")
        return

    pipeline = dlt.pipeline(
        pipeline_name="large_parquet_docker",
        destination=dlt.destinations.filesystem(
            bucket_url=os.environ.get("BUCKET_URL", "s3://warehouse/my-lake"),
            credentials={
                "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
                "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
                "endpoint_url": os.environ.get("AWS_ENDPOINT_URL_S3", "http://minio:9000"),
                "region_name": os.environ.get("AWS_REGION", "us-east-1"),
            },
        ),
        dataset_name="large_test",
    )

    resource_ = dlt.resource(
        parquet_resource(str(PARQUET_DIR), limit_files=LIMIT_FILES, batch_size=BATCH_SIZE),
        name="wide_table",
        table_format="iceberg",
        write_disposition="append",
    )

    print(f"\nStarting pipeline (limit_files={LIMIT_FILES}, batch_size={BATCH_SIZE})...")
    log_memory("before pipeline.run")

    try:
        info = pipeline.run(resource_)
        print("\n=== Load Info ===")
        print(info)
    except Exception as e:
        print(f"\n=== FAILED ===")
        print(f"Error: {e}")
    finally:
        log_memory("after pipeline.run")

    print("\n=== Peak Memory ===")
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    peak_rss_mb = rusage.ru_maxrss / 1024  # Linux: kilobytes → MB
    print(f"  Peak RSS (entire process): {peak_rss_mb:.0f} MB")
    print(f"  Arrow pool (current):      {pa.total_allocated_bytes() / 1024**2:.0f} MB")


if __name__ == "__main__":
    main()
