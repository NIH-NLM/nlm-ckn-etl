#!/usr/bin/env bash
# fetch-entrypoint.sh — entrypoint for the scheduled fetch task.
#
# Runs flows/fetch.py, which internally handles:
#   1. Restoring the external API cache from s3://S3_BUCKET/external/
#   2. Fetching from external APIs (CELLxGENE, Open Targets, NCBI, UniProt, HuBMAP)
#   3. Pushing the validated cache to s3://S3_BUCKET/runs/{run}/external-staging/
#   4. Promoting runs/{run}/external-staging/ → s3://S3_BUCKET/external/ atomically
#
# The staging → promotion split ensures a concurrent pipeline.py reading from
# external/ always sees a complete, validated snapshot.
#
# Required environment variables:
#   S3_BUCKET    — S3 bucket name (e.g. cell-kn-arangodb-data-952291113202)
#   NCBI_EMAIL   — NCBI E-Utilities email address
#   NCBI_API_KEY — NCBI E-Utilities API key
set -euo pipefail

: "${S3_BUCKET:?S3_BUCKET must be set}"
: "${NCBI_EMAIL:?NCBI_EMAIL must be set}"
: "${NCBI_API_KEY:?NCBI_API_KEY must be set}"

# --max-fetch-age-hours auto-resolves whether to force a full re-fetch: it forces
# only if the cached external data is missing, older than 4 weeks (672h), or was
# produced by changed fetch code; otherwise it reuses the cache and retries any
# previously-failed entries.  Avoids a full re-fetch on every scheduled run.
echo "=== Running fetch flow ==="
exec python /app/python/src/flows/fetch.py \
    --ncbi-email          "${NCBI_EMAIL}" \
    --ncbi-api-key        "${NCBI_API_KEY}" \
    --max-fetch-age-hours 672

echo "=== Fetch complete ==="
