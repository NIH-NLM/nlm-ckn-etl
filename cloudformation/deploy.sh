#!/usr/bin/env bash
# deploy.sh — Deploy NLM-CKN CloudFormation stacks in order.
#
# Usage:
#   ./deploy.sh [--params <file>] [--skip-ecr] [--skip-batch] [--skip-fetch]
#
#   --params <file>   Path to JSON config file (default: params.json in this directory)
#   --skip-ecr        Skip the ECR stack
#   --skip-batch      Skip the Batch stack
#   --skip-fetch      Skip the Fetch stack
#   --help
#
# To seed the S3 bucket with local data/external/ run seed-s3.sh first.
# See params.json for the expected JSON structure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARAMS_FILE="$SCRIPT_DIR/params.json"
SKIP_ECR=false
SKIP_BATCH=false
SKIP_FETCH=false

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --params)     PARAMS_FILE="$2"; shift 2 ;;
    --skip-ecr)   SKIP_ECR=true;    shift ;;
    --skip-batch) SKIP_BATCH=true;  shift ;;
    --skip-fetch) SKIP_FETCH=true;  shift ;;
    --help)       usage ;;
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

# ── Read config ───────────────────────────────────────────────────────────────
REGION="$(jq -r '.region // "us-east-1"' "$PARAMS_FILE")"
S3_BUCKET="$(jq -r '.s3_bucket // ""' "$PARAMS_FILE")"
ECR_STACK="$(jq -r '.stacks.ecr // "nlm-ckn-ecr"' "$PARAMS_FILE")"
BATCH_STACK="$(jq -r '.stacks.batch // "nlm-ckn-batch"' "$PARAMS_FILE")"
FETCH_STACK="$(jq -r '.stacks.fetch // "nlm-ckn-fetch"' "$PARAMS_FILE")"

# Convert a JSON object string to a newline-delimited "Key=Value" list
json_to_overrides() {
  jq -r 'to_entries[] | "\(.key)=\(.value)"' <<< "$1"
}

# ── Helpers ───────────────────────────────────────────────────────────────────
deploy_stack() {
  local stack_name="$1"
  local template="$2"
  shift 2
  echo ""
  echo "==> Deploying stack: $stack_name"
  aws cloudformation deploy \
    --region "$REGION" \
    --stack-name "$stack_name" \
    --template-file "$template" \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset \
    "$@"
  echo "    Done: $stack_name"
}

get_stack_output() {
  aws cloudformation describe-stacks \
    --region "$REGION" \
    --stack-name "$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
    --output text
}

# ── 1. ECR ────────────────────────────────────────────────────────────────────
if ! $SKIP_ECR; then
  deploy_stack "$ECR_STACK" "$SCRIPT_DIR/ecr.yaml"
fi

# ── 2. Fetch ──────────────────────────────────────────────────────────────────
if ! $SKIP_FETCH; then
  # Push NCBI credentials to SSM from environment variables
  if [[ -z "${NCBI_EMAIL:-}" || -z "${NCBI_API_KEY:-}" ]]; then
    echo "Error: NCBI_EMAIL and NCBI_API_KEY environment variables must be set before deploying the Fetch stack."
    exit 1
  fi

  NCBI_EMAIL_PATH="$(jq -r '.fetch.NcbiEmailSsmPath // "/nlm-ckn-etl/ncbi-email"' "$PARAMS_FILE")"
  NCBI_API_KEY_PATH="$(jq -r '.fetch.NcbiApiKeySsmPath // "/nlm-ckn-etl/ncbi-api-key"' "$PARAMS_FILE")"

  echo ""
  echo "==> Writing NCBI credentials to SSM Parameter Store"
  aws ssm put-parameter \
    --region "$REGION" \
    --name "$NCBI_EMAIL_PATH" \
    --value "$NCBI_EMAIL" \
    --type SecureString \
    --overwrite
  aws ssm put-parameter \
    --region "$REGION" \
    --name "$NCBI_API_KEY_PATH" \
    --value "$NCBI_API_KEY" \
    --type SecureString \
    --overwrite
  echo "    Done: SSM parameters written"

  fetch_json="$(jq '.fetch + {"EcrStackName": "'"$ECR_STACK"'", "S3Bucket": "'"$S3_BUCKET"'"}
    | del(.NcbiEmailSsmPath, .NcbiApiKeySsmPath)' "$PARAMS_FILE")"
  fetch_overrides=()
  while IFS= read -r line; do fetch_overrides+=("$line"); done \
    < <(json_to_overrides "$fetch_json")
  deploy_stack "$FETCH_STACK" "$SCRIPT_DIR/fetch.yaml" \
    --parameter-overrides "${fetch_overrides[@]}"
fi

# ── 3. Batch ──────────────────────────────────────────────────────────────────
if ! $SKIP_BATCH; then
  ECR_IMAGE_URI="$(get_stack_output "$ECR_STACK" "PipelineRepositoryUri"):latest"
  batch_json="$(jq '.batch + {"EcrImageUri": "'"$ECR_IMAGE_URI"'", "S3Bucket": "'"$S3_BUCKET"'"}' "$PARAMS_FILE")"
  batch_overrides=()
  while IFS= read -r line; do batch_overrides+=("$line"); done \
    < <(json_to_overrides "$batch_json")
  deploy_stack "$BATCH_STACK" "$SCRIPT_DIR/batch.yaml" \
    --parameter-overrides "${batch_overrides[@]}"
fi


echo ""
echo "All stacks deployed successfully."
echo "  ECR:   $ECR_STACK"
$SKIP_BATCH || echo "  Batch: $BATCH_STACK"
$SKIP_FETCH || echo "  Fetch: $FETCH_STACK"
