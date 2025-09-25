#!/bin/bash

# Test S3 connection with proper credentials
export AWS_ACCESS_KEY_ID="MLAn0-sy5uv5qebve9dUQFEL"
export AWS_SECRET_ACCESS_KEY="mwr3EBGY5SDO2eEft_r6m5KfPDSPFoRzv12JFQO_"

echo "Testing s5cmd with credentials..."
echo "Endpoint: http://kazan.archive.systems:9080 (note: HTTP, not HTTPS)"
echo "Bucket: backup"
echo ""

echo "1. Checking s5cmd version:"
s5cmd version
echo ""

echo "2. Listing buckets:"
s5cmd --endpoint-url http://kazan.archive.systems:9080 ls
echo ""

echo "3. Listing objects in backup bucket:"
s5cmd --endpoint-url http://kazan.archive.systems:9080 ls s3://backup/
echo ""