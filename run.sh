#!/bin/bash

# S3 Flood Run Script

echo "=== S3 Flood ==="
echo "Starting S3 Flood application..."

# Check if s5cmd is installed
if ! command -v s5cmd &> /dev/null
then
    echo "s5cmd is not installed. Please run install.sh first."
    exit 1
fi

echo "✓ s5cmd is installed"

# Activate virtual environment
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
else
    echo "Virtual environment not found. Please run install.sh first."
    exit 1
fi

# Check if Python dependencies are installed
if ! python -c "import questionary, rich, yaml" &> /dev/null
then
    echo "Python dependencies are not installed. Please run install.sh first."
    exit 1
fi

echo "✓ Python dependencies are installed"

# Run the application
echo "Starting S3 Flood TUI..."
python s3_flood.py

echo "=== S3 Flood Finished ==="