#!/usr/bin/env bash
# fetch-entrypoint.sh — entrypoint for the scheduled fetch task.
#
# 1. Restores the external API cache from S3 (enables resume on re-run).
# 2. Runs the Prefect fetch flow (fetcher.py).
# 3. Pushes the updated cache back to S3.
#
# Required environment variables:
#   S3_BUCKET       — S3 bucket name (e.g. cell-kn-arangodb-data-952291113202)
#   NCBI_EMAIL      — NCBI E-Utilities email address
#   NCBI_API_KEY    — NCBI E-Utilities API key
#
# Optional:
#   ARANGO_DB_PASSWORD — forwarded to fetcher.py (not used by the fetch flow)
set -euo pipefail

: "${S3_BUCKET:?S3_BUCKET must be set}"

EXTERNAL_DIR="/app/data/external"
mkdir -p "$EXTERNAL_DIR"

echo "=== Restoring external cache from s3://${S3_BUCKET}/external/ ==="
aws s3 sync "s3://${S3_BUCKET}/external/" "$EXTERNAL_DIR/" || {
    echo "WARNING: S3 sync failed (bucket may be empty on first run); continuing."
}

echo "=== Running fetch flow ==="
python /app/python/src/fetcher.py \
    --ncbi-email    "${NCBI_EMAIL:-}" \
    --ncbi-api-key  "${NCBI_API_KEY:-}" \
    --force

echo "=== Pushing updated cache to s3://${S3_BUCKET}/external/ ==="
aws s3 sync "$EXTERNAL_DIR/" "s3://${S3_BUCKET}/external/"

echo "=== Fetch complete ==="
