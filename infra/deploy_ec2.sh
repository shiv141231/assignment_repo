#!/usr/bin/env bash
# =============================================================================
# steps to deploy the code on ec2 instance 
# deploy_ec2.sh — Run  this on EC2
# Run this ON the EC2 instance after SSHing in.
#
# Usage:
#   ./infra/deploy_ec2.sh
#   PROCESSOR=spark ./infra/deploy_ec2.sh   # for scaling up with spark
# =============================================================================
set -euo pipefail

REGION="us-east-1"                        
BUCKET="assignment-bucket-ss"                    
INPUT_PATH="s3://${BUCKET}/input/data.sql"
OUTPUT_PATH="s3://${BUCKET}/output/"
PROCESSOR="${PROCESSOR:-chunked}"
WORK_DIR="/tmp/assignment_run"

echo "=================================================="
echo " assignment — EC2 Run"
echo " Bucket  : $BUCKET"
echo " Input   : $INPUT_PATH"
echo " Backend : $PROCESSOR"
echo "=================================================="

# 1. Python
echo "[1/6] Checking Python..."
if ! command -v python3 &>/dev/null; then
    if [ -f /etc/system-release ]; then
        sudo yum update -y -q && sudo yum install -y python3 python3-pip
    else
        sudo apt-get update -q && sudo apt-get install -y python3 python3-pip
    fi
fi
python3 --version
pip3 --version || python3 -m ensurepip --upgrade

# 2. AWS CLI
echo "[2/6] Checking AWS CLI..."
if ! command -v aws &>/dev/null; then
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
    unzip -q /tmp/awscliv2.zip -d /tmp && sudo /tmp/aws/install
    rm -rf /tmp/awscliv2.zip /tmp/aws
fi
aws configure set region "$REGION"

# 3. PySpark (for the scaling part set PROCESSOR=spark)
echo "[3/6] Checking dependencies..."
if [ "$PROCESSOR" = "spark" ]; then
    pip3 install --quiet pyspark && echo "PySpark ready."
else
    echo "Chunked backend — no extra dependencies."
fi

# 4. Working directory
echo "[4/6] Setting up working directory..."
rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp -r "$SCRIPT_DIR/../src"     "$WORK_DIR/src"
cp    "$SCRIPT_DIR/../main.py" "$WORK_DIR/main.py"

# 5. Download input from S3
echo "[5/6] Downloading from S3..."
aws s3 cp "$INPUT_PATH" "$WORK_DIR/data.sql" --region "$REGION"

# 6. Run
echo "[6/6] Running analyzer..."
cd "$WORK_DIR"
PROCESSOR="$PROCESSOR" python3 main.py data.sql

# 7. Upload output
echo "Uploading output to S3..."
OUTPUT_FILE=$(ls "$WORK_DIR"/*_SearchKeywordPerformance.tab 2>/dev/null | head -1)
[ -z "$OUTPUT_FILE" ] && { echo "ERROR: No .tab file found"; exit 1; }
aws s3 cp "$OUTPUT_FILE" "${OUTPUT_PATH%/}/$(basename "$OUTPUT_FILE")" --region "$REGION"

echo ""
echo "=================================================="
echo "Done! Output: ${OUTPUT_PATH%/}/$(basename "$OUTPUT_FILE")"
echo "Download: aws s3 cp ${OUTPUT_PATH%/}/$(basename "$OUTPUT_FILE") ./"
echo "=================================================="
