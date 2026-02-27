#!/usr/bin/env bash
# =============================================================================
# deploy_glue.sh — Glue job via CloudFormation
# Usage:
#   ./infra/deploy_glue.sh
# =============================================================================
set -euo pipefail

REGION="us-east-1"          
BUCKET="assignment-bucket-ss"       
STACK_NAME="assignment-glue-stack"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo " assignment — Glue Deploy"
echo " Region  : $REGION"
echo " Bucket  : $BUCKET"
echo " Stack   : $STACK_NAME"
echo "=================================================="

echo "Checking AWS credentials..."
aws sts get-caller-identity --region "$REGION" > /dev/null || {
    echo "ERROR: Run 'aws configure' first"; exit 1
}

# 1. Package src/
echo "[1/4] Packaging src/ → src.zip..."
cd "$PROJECT_DIR"
rm -f src.zip
zip -r src.zip src/ --exclude "src/__pycache__/*" --exclude "src/*.pyc"

# 2. Uploading scripts
echo "[2/4] Uploading scripts to s3://${BUCKET}/scripts/ ..."
aws s3 cp glue_job.py "s3://${BUCKET}/scripts/glue_job.py" --region "$REGION"
aws s3 cp src.zip     "s3://${BUCKET}/scripts/src.zip"     --region "$REGION"

# 3. Deploying CloudFormation
echo "[3/4] Deploying CloudFormation..."
aws cloudformation deploy \
    --region "$REGION" \
    --stack-name "$STACK_NAME" \
    --template-file "$SCRIPT_DIR/glue_stack.yaml" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        ProjectName="$STACK_NAME" \
        DataBucketName="$BUCKET" \
        ScriptBucketName="$BUCKET"

# 4. Print result
echo "[4/4] Done!"
JOB_NAME=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueJobName'].OutputValue" \
    --output text)
rm -f src.zip

echo ""
echo "=================================================="
echo " Glue job deployed: $JOB_NAME"
echo ""
echo "Run the job:"
echo "  aws glue start-job-run --job-name '$JOB_NAME' --region $REGION"
echo ""
echo "Check status:"
echo "  aws glue get-job-runs --job-name '$JOB_NAME' --region $REGION \\"
echo "    --query 'JobRuns[0].{State:JobRunState,Duration:ExecutionTime}'"
echo ""
echo "Download result:"
echo "  aws s3 cp s3://${BUCKET}/output/ ./ --recursive --include '*.tab'"
echo "=================================================="
