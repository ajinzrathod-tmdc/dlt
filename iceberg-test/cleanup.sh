#!/usr/bin/env bash
# Cleanup script: removes dlt-generated data + Iceberg catalog state.
#
# Usage (from inside the container):
#   bash cleanup.sh
#
# Usage (from your Mac):
#   docker exec dlt-loader bash cleanup.sh

set -e

echo "================================================"
echo "================================================"
echo "================================================"
echo "=== Cleaning up dlt-generated data ==="


# 1. Remove dlt pipeline working directories
echo "Removing dlt pipeline local state..."
rm -rf /root/.dlt/pipelines/*
rm -rf /var/dlt/pipelines/*
rm -rf /tmp/dlt_*
rm -rf /app/*.duckdb /app/*.duckdb.wal
echo "  Cleaned: pipeline state, temp files"

# 2. Drop tables from the Iceberg REST catalog
echo "Dropping Iceberg tables from catalog..."
python3 -c "
from pyiceberg.catalog import load_catalog

catalog = load_catalog('default')

for ns in catalog.list_namespaces():
    ns_name = ns[0]
    for table_id in catalog.list_tables(ns_name):
        full_name = f'{table_id[0]}.{table_id[1]}'
        try:
            catalog.drop_table(full_name, purge_requested=True)
            print(f'  Dropped table: {full_name}')
        except Exception as e:
            print(f'  Warning dropping {full_name}: {e}')
    try:
        catalog.drop_namespace(ns_name)
        print(f'  Dropped namespace: {ns_name}')
    except Exception as e:
        print(f'  Warning dropping namespace {ns_name}: {e}')

print('  Catalog cleaned')
" 2>/dev/null || echo "  Warning: could not reach Iceberg catalog (is it running?)"

# 3. Wipe the MinIO warehouse bucket
echo "Wiping MinIO warehouse bucket..."
python3 -c "
import boto3
from botocore.config import Config
import os

s3 = boto3.client(
    's3',
    endpoint_url=os.environ.get('AWS_ENDPOINT_URL_S3', 'http://minio:9000'),
    aws_access_key_id='admin',
    aws_secret_access_key='password',
    region_name='us-east-1',
    config=Config(signature_version='s3v4'),
)

bucket = 'warehouse'
try:
    objects = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' in objects:
        keys = [{'Key': obj['Key']} for obj in objects['Contents']]
        while objects.get('IsTruncated'):
            objects = s3.list_objects_v2(
                Bucket=bucket,
                ContinuationToken=objects['NextContinuationToken'],
            )
            keys.extend({'Key': obj['Key']} for obj in objects['Contents'])

        for i in range(0, len(keys), 1000):
            batch = keys[i:i+1000]
            s3.delete_objects(Bucket=bucket, Delete={'Objects': batch})
        print(f'  Deleted {len(keys)} objects from s3://{bucket}/')
    else:
        print(f'  Bucket s3://{bucket}/ is already empty')
except Exception as e:
    print(f'  Warning: {e}')
"

echo ""
echo "=== Done ==="
echo "  Kept: /app/parquet_output/ (your source data)"
echo "  Removed: dlt pipelines, Iceberg catalog tables, MinIO warehouse contents"
echo ""
echo "You can now re-run the pipeline fresh."
