#!/bin/bash

# Test script with credentials
# Set your S3 credentials here or export them in your environment

export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"

# Configuration
S3_URL="http://localhost:9000"
BUCKET_NAME="test-bucket"

echo "=== S3 Flood Test with Credentials ==="
echo "S3 URL: $S3_URL"
echo "Access Key: ${AWS_ACCESS_KEY_ID:0:5}..."  # Show only first 5 characters
echo "Secret Key length: ${#AWS_SECRET_ACCESS_KEY} characters"
echo "Bucket: $BUCKET_NAME"

# Run the test
python3 run_test.py