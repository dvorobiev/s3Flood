#!/bin/bash

# S3 Flood Installation Script

echo "=== S3 Flood Installation ==="
echo "This script will install all required dependencies for S3 Flood"

# Check if Python is installed
if ! command -v python3 &> /dev/null
then
    echo "Python 3 is not installed. Please install Python 3.7 or higher."
    exit 1
fi

echo "✓ Python 3 is installed"

# Check if pip is installed
if ! command -v pip3 &> /dev/null
then
    echo "pip3 is not installed. Please install pip3."
    exit 1
fi

echo "✓ pip3 is installed"

# Install s5cmd
echo "Installing s5cmd..."
if command -v brew &> /dev/null
then
    # macOS with Homebrew
    brew tap peak/tap
    brew install s5cmd
elif command -v apt-get &> /dev/null
then
    # Ubuntu/Debian
    echo "Installing s5cmd on Ubuntu/Debian..."
    wget -O s5cmd.tar.gz https://github.com/peak/s5cmd/releases/latest/download/s5cmd_$(uname -s)_$(uname -m).tar.gz
    tar -xzf s5cmd.tar.gz
    sudo install s5cmd /usr/local/bin/
    rm s5cmd.tar.gz
    echo "✓ s5cmd installed successfully"
elif command -v yum &> /dev/null
then
    # CentOS/RHEL
    echo "Installing s5cmd on CentOS/RHEL..."
    wget -O s5cmd.tar.gz https://github.com/peak/s5cmd/releases/latest/download/s5cmd_$(uname -s)_$(uname -m).tar.gz
    tar -xzf s5cmd.tar.gz
    sudo install s5cmd /usr/local/bin/
    rm s5cmd.tar.gz
    echo "✓ s5cmd installed successfully"
else
    echo "Please install s5cmd manually from https://github.com/peak/s5cmd"
    echo "Or use: wget -O s5cmd https://github.com/peak/s5cmd/releases/latest/download/s5cmd_$(uname -s)_$(uname -m).tar.gz && tar -xzf s5cmd_$(uname -s)_$(uname -m).tar.gz && sudo install s5cmd /usr/local/bin/"
fi

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "=== Installation Complete ==="
echo "To run the application, first activate the virtual environment:"
echo "  source venv/bin/activate"
echo "Then run:"
echo "  python s3_flood.py --config"
echo "Or:"
echo "  python s3_flood.py"