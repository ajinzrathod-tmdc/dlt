"""
Load real parquet files from parquet_output/ into Iceberg on MinIO via dlt.

Usage:
  # Load 1 file (~550 MB) — should work
  uv run python iceberg-test/test_large_parquet.py --files 1

  # Load 3 files (~1.6 GB) — will likely OOM on 8GB machine
  uv run python iceberg-test/test_large_parquet.py --files 3

  # Load all 26 files (~14 GB) — will definitely OOM
  uv run python iceberg-test/test_large_parquet.py

Prerequisites:
  cd iceberg-test && docker compose up -d && source ./env.sh && cd ..
"""

import argparse
import os
import tracemalloc
from pathlib import Path

import pyarrow.dataset as ds
import dlt


PARQUET_DIR = Path(__file__).parent / "parquet_output"


def parquet_resource(parquet_dir: str, limit_files: int | None = None, batch_size: int = 10_000):
    """Yield Arrow RecordBatches from parquet files in a directory."""
    parquet_files = sorted(Path(parquet_dir).glob("*.parquet"))
    if limit_files:
        parquet_files = parquet_files[:limit_files]

    total_mb = sum(f.stat().st_size for f in parquet_files) / (1024**2)
    print(f"Loading {len(parquet_files)} files ({total_mb:.0f} MB) from {parquet_dir}")

    dataset = ds.dataset([str(f) for f in parquet_files], format="parquet")
    for batch in dataset.to_batches(batch_size=batch_size):
        yield batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=None, help="Limit number of parquet files to load")
    parser.add_argument("--batch-size", type=int, default=10_000, help="Arrow batch size (rows)")
    args = parser.parse_args()

    if not PARQUET_DIR.exists():
        print(f"parquet_output not found at {PARQUET_DIR}")
        print("Run the parquet generator script first.")
        return

    tracemalloc.start()

    pipeline = dlt.pipeline(
        pipeline_name="large_parquet_test",
        destination=dlt.destinations.filesystem(
            bucket_url="s3://warehouse/my-lake",
            credentials={
                "aws_access_key_id": "admin",
                "aws_secret_access_key": "password",
                "endpoint_url": "http://localhost:9000",
                "region_name": "us-east-1",
            },
        ),
        dataset_name="large_test",
    )

    resource = dlt.resource(
        parquet_resource(str(PARQUET_DIR), limit_files=args.files, batch_size=args.batch_size),
        name="wide_table",
        table_format="iceberg",
        write_disposition="append",
    )

    print(f"\nStarting pipeline...")
    current, peak = tracemalloc.get_traced_memory()
    print(f"  Memory before run: current={current / 1024**2:.1f} MB, peak={peak / 1024**2:.1f} MB")

    try:
        info = pipeline.run(resource)
        print("\n=== Load Info ===")
        print(info)
    except Exception as e:
        print(f"\n=== FAILED ===")
        print(f"Error: {e}")
    finally:
        current, peak = tracemalloc.get_traced_memory()
        print(f"\n=== Memory Usage ===")
        print(f"  Current: {current / 1024**2:.1f} MB")
        print(f"  Peak:    {peak / 1024**2:.1f} MB")
        tracemalloc.stop()


if __name__ == "__main__":
    main()
