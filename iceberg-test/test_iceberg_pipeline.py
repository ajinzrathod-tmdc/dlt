"""
Minimal dlt pipeline: load parquet data into Iceberg table on MinIO via REST catalog.

Prerequisites:
  1. cd iceberg-test && docker compose up -d
  2. source ./env.sh
  3. uv run python iceberg-test/test_iceberg_pipeline.py   (from the dlt repo root)

Data flow:
  Step 1: Generate a sample parquet file (simulating real source data)
  Step 2: Read it via Arrow and feed it to dlt as a resource
  Step 3: dlt normalizes → uploads to MinIO → commits to Iceberg table
"""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

import dlt


# ── Step 1: Create a sample parquet file (simulate your real data source) ──

def create_sample_parquet(path: str, num_rows: int = 10_000) -> str:
    """Generate a parquet file with sample data. Replace this with your real parquet path."""
    table = pa.table({
        "id": pa.array(range(num_rows), type=pa.int64()),
        "name": pa.array([f"person_{i}" for i in range(num_rows)], type=pa.string()),
        "value": pa.array([i * 1.5 for i in range(num_rows)], type=pa.float64()),
        "category": pa.array([f"cat_{i % 5}" for i in range(num_rows)], type=pa.string()),
    })
    pq.write_table(table, path)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"Created sample parquet: {path} ({num_rows:,} rows, {size_mb:.1f} MB)")
    return path


# ── Step 2: dlt resource that yields Arrow tables from parquet ──

def parquet_resource(parquet_path: str, batch_size: int = 2_000):
    """Read a parquet file in batches and yield Arrow tables to dlt."""
    parquet_file = pq.ParquetFile(parquet_path)
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        yield batch


# ── Step 3: Run the pipeline ──

def main():
    # Create sample parquet (swap this path for your real data)
    tmp_dir = tempfile.mkdtemp(prefix="dlt_parquet_source_")
    parquet_path = os.path.join(tmp_dir, "sample_data.parquet")
    create_sample_parquet(parquet_path, num_rows=10_000)

    pipeline = dlt.pipeline(
        pipeline_name="iceberg_test",
        destination=dlt.destinations.filesystem(
            bucket_url="s3://warehouse/my-lake",
            credentials={
                "aws_access_key_id": "admin",
                "aws_secret_access_key": "password",
                "endpoint_url": "http://localhost:9000",
                "region_name": "us-east-1",
            },
        ),
        dataset_name="test_dataset",
    )

    # Wrap as a dlt resource with iceberg table format
    resource = dlt.resource(
        parquet_resource(parquet_path),
        name="people",
        table_format="iceberg",
        write_disposition="append",
    )

    info = pipeline.run(resource)
    print("\n=== Load Info ===")
    print(info)

    # ── Inspect: snapshots, parquet files, data ──
    print("\n=== Inspecting Iceberg Tables ===")
    from dlt.common.libs.pyiceberg import get_iceberg_tables

    tables = get_iceberg_tables(pipeline, "people")
    for name, table in tables.items():
        print(f"\n--- Table: {name} ---")
        print(f"  Location: {table.location()}")
        print(f"  Schema:   {table.schema()}")
        print(f"  Snapshots:")
        for snapshot in table.history():
            print(f"    {snapshot}")

        if table.current_snapshot():
            files = [task.file.file_path for task in table.scan().plan_files()]
            print(f"  Data files ({len(files)}):")
            for f in files:
                print(f"    {f}")

        df = table.scan().to_pandas()
        print(f"  Total rows: {len(df)}")
        print(df.head(10).to_string(index=False))

    # Cleanup temp parquet
    os.remove(parquet_path)
    os.rmdir(tmp_dir)


if __name__ == "__main__":
    main()
