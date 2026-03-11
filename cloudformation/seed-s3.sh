#!/usr/bin/env bash
# seed-s3.sh — Upload local data/external/ to the S3 bucket.
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

if [[ ! -d "$LOCAL_EXTERNAL" ]]; then
  echo "Error: $LOCAL_EXTERNAL not found"
  exit 1
fi

echo "==> Syncing $LOCAL_EXTERNAL → s3://$S3_BUCKET/external/"
aws s3 sync "$LOCAL_EXTERNAL/" "s3://$S3_BUCKET/external/" \
  --region "$REGION" \
  --exclude "*.DS_Store"
echo "Done."
