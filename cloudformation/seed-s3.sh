#!/usr/bin/env bash
# seed-s3.sh — Upload local data to the S3 bucket.
#
# Uploads:
#   data/external/      → s3://bucket/external/     (external API cache)
#   data/results-*/     → s3://bucket/results/<dir>/ (NSForest results)
#   data/obo/           → s3://bucket/obo/           (OWL files + generated text files)
#
# data/obo/ contains OWL files downloaded by OntologyDownloader (cl.owl,
# ro.owl, etc.) and text files written by OntologyGraphBuilder
# (deprecated_terms.txt, edge_labels.txt).  The Batch container syncs them
# via sync_obo_from_s3() before building the results and phenotype graphs.
#
# Usage:
#   ./seed-s3.sh [--params <file>]
#
#   --params <file>   Path to JSON config file (default: params.json in this directory)
#   --help
#
# Reads s3_bucket and region from params.json.
# Safe to re-run; aws s3 sync only uploads new or changed files.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARAMS_FILE="$SCRIPT_DIR/params.json"

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --params) PARAMS_FILE="$2"; shift 2 ;;
    --help)   usage ;;
    *) echo "Unknown option: $1"; usage ;;
  esac
done

if [[ ! -f "$PARAMS_FILE" ]]; then
  echo "Error: params file not found: $PARAMS_FILE"
  exit 1
fi

if ! command -v jq &>/dev/null; then
  echo "Error: jq is required (brew install jq)"
  exit 1
fi

REGION="$(jq -r '.region // "us-east-1"' "$PARAMS_FILE")"
S3_BUCKET="$(jq -r '.s3_bucket // ""' "$PARAMS_FILE")"

if [[ -z "$S3_BUCKET" ]]; then
  echo "Error: s3_bucket not set in $PARAMS_FILE"
  exit 1
fi

REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL_EXTERNAL="$REPO_ROOT/data/external"
LOCAL_DATA="$REPO_ROOT/data"

if [[ ! -d "$LOCAL_EXTERNAL" ]]; then
  echo "Error: $LOCAL_EXTERNAL not found"
  exit 1
fi

echo "==> Syncing $LOCAL_EXTERNAL → s3://$S3_BUCKET/external/"
aws s3 sync "$LOCAL_EXTERNAL/" "s3://$S3_BUCKET/external/" \
  --region "$REGION" \
  --exclude "*.DS_Store"

# Upload NSForest results directories (results-*) so the pipeline container
# can sync them via sync_results_from_s3() (expects s3://bucket/results/<dir>/).
results_dirs=("$LOCAL_DATA"/results-*)
if [[ -d "${results_dirs[0]}" ]]; then
  for results_dir in "${results_dirs[@]}"; do
    if [[ -d $results_dir ]]; then
      dir_name="$(basename "$results_dir")"
      echo "==> Syncing $results_dir → s3://$S3_BUCKET/results/$dir_name/"
      aws s3 sync "$results_dir/" "s3://$S3_BUCKET/results/$dir_name/" \
        --region "$REGION" \
        --exclude "*.DS_Store"
    else
      echo "Skipping $results_dir, not directory"
    fi
  done
else
  echo "WARNING: No data/results-* directories found; skipping results upload."
fi

# Upload all OBO files (OWL ontology files + generated text files) so the
# Batch container can sync them via sync_obo_from_s3() before building the
# results and phenotype graphs (ResultsGraphBuilder requires ro.owl etc.).
LOCAL_OBO="$LOCAL_DATA/obo"
if [[ -d "$LOCAL_OBO" ]]; then
  echo "==> Syncing $LOCAL_OBO → s3://$S3_BUCKET/obo/"
  aws s3 sync "$LOCAL_OBO/" "s3://$S3_BUCKET/obo/" \
    --region "$REGION" \
    --exclude "*.DS_Store"
else
  echo "WARNING: $LOCAL_OBO not found; skipping obo upload."
fi

echo "Done."
