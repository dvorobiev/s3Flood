# S3 Flood - Complete Instructions

## Table of Contents
1. [Overview](#overview)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Features](#features)
6. [Tools](#tools)
7. [Release Notes](#release-notes)

## Overview

S3 Flood is a cross-platform terminal interface (TUI) application designed for stress-testing S3-compatible storage systems such as MinIO or AWS S3. It evaluates performance by generating test files and performing parallel upload, download, and delete operations.

S3 Flood поддерживает несколько инструментов для работы с S3:
- **s5cmd** - основной инструмент (по умолчанию)
- **rclone** - альтернативный инструмент с расширенными возможностями

## Installation

### Linux/macOS

1. Clone or download this repository
2. Run the installation script:
   ```bash
   ./install.sh
   ```

### Windows

1. **Requirements:**
   - Python 3.7+ (download from [python.org](https://www.python.org/downloads/))
   - When installing Python, be sure to check "Add Python to PATH"

2. **Automatic Installation:**
   - Download the project from GitHub
   - Run `install.bat` as administrator
   - The script will automatically install all dependencies

3. **Manual Installation:**
   ```cmd
   # Install Python dependencies
   pip install -r requirements.txt
   
   # Download s5cmd for Windows
   mkdir tools
   # Download s5cmd from https://github.com/peak/s5cmd/releases
   # Extract s5cmd.exe to the tools/ folder
   
   # Optional: Download rclone for Windows
   # Download rclone from https://rclone.org/downloads/
   # Extract rclone.exe to the tools/ folder
   ```

### PowerShell Version

For Windows users, there's also a native PowerShell implementation:
- **s3_flood_powershell.ps1** - Complete PowerShell rewrite with full feature parity
- **install_powershell.ps1** - Automated installation script for PowerShell environment

## Configuration

Before running the application, configure it with your S3 parameters:

```bash
source venv/bin/activate
python s3_flood.py --config
```

This will launch an interactive setup wizard where you can specify:
- S3 endpoint URLs
- Access key and secret key
- Bucket name
- Number of parallel threads
- File group sizes and counts
- Infinite loop settings
- Cluster mode settings

Example config.yaml:
```yaml
s3_urls:
  - http://localhost:9000        # S3 endpoint URLs (can be multiple for cluster mode)
access_key: YOUR_ACCESS_KEY     # Your S3 access key
secret_key: YOUR_SECRET_KEY     # Your S3 secret key
bucket_name: test-bucket        # Name of the bucket to use for testing
cluster_mode: false             # Enable cluster mode for multiple endpoints
parallel_threads: 5             # Number of parallel threads for operations

# File groups configuration
file_groups:
  small:
    max_size_mb: 100            # Maximum size for small files (in MB)
    count: 100                  # Number of small files to create
  medium:
    max_size_mb: 5120           # Maximum size for medium files (in MB)
    count: 50                   # Number of medium files to create
  large:
    max_size_mb: 20480          # Maximum size for large files (in MB)
    count: 10                   # Number of large files to create

# Test cycle configuration
infinite_loop: true             # Run tests in an infinite loop
cycle_delay_seconds: 15         # Delay between test cycles (in seconds)

# Test files location
test_files_directory: "./s3_temp_files"  # Directory for temporary test files
```

## Usage

### Python Version

1. Activate virtual environment:
   ```bash
   source venv/bin/activate  # Linux/macOS
   # or
   venv\Scripts\activate     # Windows
   ```

2. Run the application:
   ```bash
   python s3_flood.py
   ```

3. Select tool (s5cmd or rclone) when prompted
4. Follow the interactive menu

### PowerShell Version

1. Download rclone for Windows from [https://rclone.org/downloads/](https://rclone.org/downloads/)
2. Extract `rclone.exe` to the same directory as the script
3. Configure rclone:
   ```bash
   ./rclone config
   ```
   Create a remote named `demo` (or change `$rcloneRemote` in the script)
4. Run the script:
   ```powershell
   .\s3_flood_powershell.ps1
   ```

## Features

### Core Features
- **Interactive TUI**: Easy setup and real-time monitoring
- **Customizable File Generation**: Creation of test files of various sizes (small, medium, large)
- **Parallel Operations**: Concurrent execution of S3 operations
- **File Progress Tracking**: Individual progress indicators for each operation
- **Randomized Load**: Random parallel read/write operations
- **Statistics Collection**: Performance metrics including throughput and operation times
- **Infinite Loop Mode**: Ability to run continuous stress testing
- **Cluster Mode**: Test multiple S3 endpoints simultaneously

### Advanced Features
- **Tool Selection**: Choose between s5cmd and rclone at startup
- **Automatic Configuration**: Automatic rclone configuration from app settings
- **Cross-Platform Support**: Works on Linux, Windows, macOS
- **Bilingual Interface**: English and Russian language support

## Tools

### s5cmd
Default tool for S3 operations. Fast and lightweight.
- Pros: High performance, low overhead
- Cons: Limited S3 provider support

### rclone
Alternative tool with extensive S3 provider support.
- Pros: Wide provider compatibility, advanced features
- Cons: Slightly higher overhead

When you select rclone, the application automatically:
1. Creates/updates rclone configuration from your app settings
2. Uses the configured S3 credentials and endpoints
3. Performs all operations through rclone commands

## Release Notes

### v1.6.3 - Rclone Support and Codebase Improvements
- Added rclone as alternative tool for S3 operations
- Tool selection menu to choose between s5cmd and rclone
- Automatic rclone configuration from application config
- Full feature parity with s5cmd implementation

### v1.6.2 - GitHub Badges
- Added badges for version, license, platform support, and Python version
- Enhanced project visibility with professional appearance

### v1.6.1 - Bilingual Documentation
- Fully bilingual documentation (English/Russian)
- Bilingual comments and messages in all scripts

### v1.6.0 - PowerShell Implementation
- Complete PowerShell rewrite with full feature parity
- Automated installation script for PowerShell environment

### v1.5.2 - Enhanced Statistics and Performance
- Detailed performance metrics and real-time monitoring
- Optimized file generation and parallel processing

### v1.5.0 - Cluster Mode and Infinite Loop
- Multi-endpoint testing with cluster mode support
- Continuous testing with infinite loop mode

### v1.0.0 - Initial Release
- Basic TUI application with core S3 testing features
- Cross-platform support for Linux, Windows, and macOS