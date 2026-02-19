#!/usr/bin/env bash
# Source this file before running the pipeline:
#   source ./env.sh

# PyIceberg REST catalog config (picked up by pyiceberg's load_catalog)
export PYICEBERG_CATALOG__DEFAULT__URI=http://localhost:8181
export PYICEBERG_CATALOG__DEFAULT__WAREHOUSE=s3://warehouse
export PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT=http://localhost:9000
export PYICEBERG_CATALOG__DEFAULT__S3__ACCESS_KEY_ID=admin
export PYICEBERG_CATALOG__DEFAULT__S3__SECRET_ACCESS_KEY=password
export PYICEBERG_CATALOG__DEFAULT__S3__REGION=us-east-1
export PYICEBERG_CATALOG__DEFAULT__S3__PATH_STYLE_ACCESS=true

# dlt Iceberg catalog selection
export ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE=rest
export ICEBERG_CATALOG__ICEBERG_CATALOG_NAME=default

echo "Environment set for MinIO + Iceberg REST catalog (localhost)"
