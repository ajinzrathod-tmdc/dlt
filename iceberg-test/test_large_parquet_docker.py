"""
Test dlt pipeline with large parquet files → Iceberg on MinIO.
dlt handles all streaming internally — no manual S3 writes needed.
"""

import gc
import os
import resource
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import logging
os.environ["RUNTIME__LOG_LEVEL"] = "INFO"
import dlt

PARQUET_DIR = Path("/app/parquet_output")
LIMIT_FILES = int(os.environ.get("LIMIT_FILES", "0")) or None
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10000"))


def get_memory_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def log_memory(label: str) -> None:
    print(
        f"  [{label}] RSS: {get_memory_mb():.0f} MB"
        f" | Arrow pool: {pa.total_allocated_bytes() / 1024**2:.0f} MB",
        flush=True,
    )


def parquet_resource(parquet_dir: str, limit_files: int | None = None, batch_size: int = 10_000):
    """Yield one batch at a time from parquet files. One file at a time, no readahead."""
    parquet_files = sorted(Path(parquet_dir).glob("*.parquet"))
    if limit_files:
        parquet_files = parquet_files[:limit_files]

    total_mb = sum(f.stat().st_size for f in parquet_files) / (1024**2)
    n_files = len(parquet_files)
    print(f"[extract] Loading {n_files} files ({total_mb:.0f} MB) from {parquet_dir}", flush=True)

    total_rows = 0
    t0 = time.monotonic()
    last_log = t0

    for file_idx, pf_path in enumerate(parquet_files):
        file_mb = pf_path.stat().st_size / (1024**2)
        print(
            f"[extract] File {file_idx + 1}/{n_files}: {pf_path.name} ({file_mb:.0f} MB)",
            flush=True,
        )

        pf = pq.ParquetFile(str(pf_path))
        file_rows = 0

        for batch in pf.iter_batches(batch_size=batch_size):
            file_rows += batch.num_rows
            total_rows += batch.num_rows
            yield batch

            now = time.monotonic()
            if now - last_log >= 10:
                elapsed = now - t0
                rate = total_rows / elapsed if elapsed > 0 else 0
                print(
                    f"  [extract progress] {total_rows:,} rows | {elapsed:.0f}s"
                    f" | {rate:,.0f} rows/s | RSS {get_memory_mb():.0f} MB",
                    flush=True,
                )
                last_log = now

        del pf
        gc.collect()
        print(
            f"  [extract] Done file {file_idx + 1}/{n_files}: {file_rows:,} rows"
            f" | total so far: {total_rows:,} | RSS {get_memory_mb():.0f} MB",
            flush=True,
        )

    elapsed = time.monotonic() - t0
    print(
        f"[extract] Finished all {n_files} files: {total_rows:,} rows in {elapsed:.1f}s",
        flush=True,
    )


def main():
    if not PARQUET_DIR.exists() or not list(PARQUET_DIR.glob("*.parquet")):
        print(f"No parquet files found at {PARQUET_DIR}")
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

    print(f"\nStarting pipeline (limit_files={LIMIT_FILES}, batch_size={BATCH_SIZE})...", flush=True)
    log_memory("before pipeline.run")

    t_start = time.monotonic()
    try:
        info = pipeline.run(resource_)
        elapsed = time.monotonic() - t_start
        print(f"\n=== Load Info ({elapsed:.1f}s total) ===", flush=True)
        print(info, flush=True)
    except Exception as e:
        elapsed = time.monotonic() - t_start
        print(f"\n=== FAILED after {elapsed:.1f}s ===", flush=True)
        print(f"Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        log_memory("after pipeline.run")
        print(f"\n=== Peak Memory ===", flush=True)
        print(f"  Peak RSS: {get_memory_mb():.0f} MB", flush=True)


if __name__ == "__main__":
    main()
