#!/bin/bash

# Script to start MinIO for testing
echo "Starting MinIO server for testing..."
echo "Make sure Docker is installed and running"

# Run MinIO in a Docker container
docker run -d \
  --name minio-test \
  -p 9000:9000 \
  -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"

echo "MinIO is starting..."
echo "Access Key: minioadmin"
echo "Secret Key: minioadmin"
echo "S3 Endpoint: http://localhost:9000"
echo "Web Console: http://localhost:9001"
echo ""
echo "Waiting for MinIO to be ready..."
sleep 5

# Create the test bucket
echo "Creating test bucket..."
docker exec minio-test mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec minio-test mc mb myminio/test-bucket

echo "MinIO is ready for testing!"
echo "Run the S3 Flood test with: python s3_flood.py"