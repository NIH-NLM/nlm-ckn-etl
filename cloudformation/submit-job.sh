#!/usr/bin/env bash
# submit-job.sh — Submit an NLM-CKN ETL job to AWS Batch.
#
# Usage:
#   ./submit-job.sh [--params <file>] [--stages <stages>]
#
#   --params <file>    Path to JSON config file (default: params.json in this directory)
#   --stages <stages>  Pipeline stages to run (default: --run-results)
#                      Examples:
#                        --stages "--run-ontology"
#                        --stages "--run-results"
#                        --stages "--run-ontology --run-results --run-archive"
#   --help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARAMS_FILE="$SCRIPT_DIR/params.json"
STAGES="--run-results"

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --params)  PARAMS_FILE="$2"; shift 2 ;;
    --stages)  STAGES="$2";      shift 2 ;;
    --help)    usage ;;
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
BATCH_STACK="$(jq -r '.stacks.batch // "nlm-ckn-batch"' "$PARAMS_FILE")"
JAVA_OPTS="$(jq -r '.batch.JavaOpts // "-Xmx4g"' "$PARAMS_FILE")"

JOB_QUEUE="$(aws cloudformation describe-stacks \
  --region "$REGION" \
  --stack-name "$BATCH_STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='JobQueueName'].OutputValue" \
  --output text)"

JOB_DEF="$(aws cloudformation describe-stacks \
  --region "$REGION" \
  --stack-name "$BATCH_STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='JobDefinitionArn'].OutputValue" \
  --output text)"

JOB_NAME="nlm-ckn-etl-$(date +%Y%m%d-%H%M%S)"

# Build the command array from the stages string
read -ra STAGE_ARGS <<< "$STAGES"
COMMAND='["pipeline.py"'
for arg in "${STAGE_ARGS[@]}"; do
  COMMAND="$COMMAND,\"$arg\""
done
COMMAND="$COMMAND,\"--java-opts=$JAVA_OPTS\"]"

echo "Job name:       $JOB_NAME"
echo "Queue:          $JOB_QUEUE"
echo "Job definition: $JOB_DEF"
echo "Stages:         $STAGES"
echo "Java opts:      $JAVA_OPTS"
echo ""

JOB_ID="$(aws batch submit-job \
  --region "$REGION" \
  --job-name "$JOB_NAME" \
  --job-queue "$JOB_QUEUE" \
  --job-definition "$JOB_DEF" \
  --container-overrides "{\"command\":$COMMAND}" \
  --query "jobId" \
  --output text)"

echo "Submitted job: $JOB_ID"
echo ""
echo "Monitor:"
echo "  aws batch describe-jobs --jobs $JOB_ID --region $REGION"
echo "  aws logs tail /batch/$BATCH_STACK-etl --follow --region $REGION"
