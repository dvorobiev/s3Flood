# S3 Flood - S3 Backend Load Testing Tool

S3 Flood is a Terminal User Interface (TUI) application designed for stress-testing S3-compatible storage backends such as MinIO or AWS S3. It enables performance evaluation by generating test files and performing parallel upload, download, and delete operations.

## Features

- **Interactive TUI**: Easy configuration and real-time monitoring
- **Configurable File Generation**: Create test files of different sizes (small, medium, large)
- **Parallel Operations**: Execute S3 operations in parallel using s5cmd
- **Per-File Progress Tracking**: Individual progress bars for each file operation
- **Randomized Workload**: Random concurrent read/write operations
- **Statistics Tracking**: Performance metrics including throughput and operation times
- **Infinite Loop Mode**: Continuous stress testing capability

## Prerequisites

- Python 3.7+
- s5cmd installed and available in PATH

## Installation

1. Clone or download this repository

2. Run the installation script:
   ```bash
   ./install.sh
   ```

   This script will:
   - Check for Python 3 and pip3
   - Install s5cmd (on macOS with Homebrew)
   - Install Python dependencies from requirements.txt

3. Alternatively, you can install dependencies manually:
   ```bash
   # Install s5cmd (varies by OS)
   # On macOS with Homebrew:
   brew tap peak/tap
   brew install s5cmd
   
   # On Linux:
   wget -O s5cmd https://github.com/peak/s5cmd/releases/latest/download/s5cmd_$(uname -s)_$(uname -m).tar.gz
   tar -xzf s5cmd_$(uname -s)_$(uname -m).tar.gz
   sudo install s5cmd /usr/local/bin/
   
   # Install Python dependencies
   pip3 install -r requirements.txt
   ```

## Configuration

Before running the application, configure it with your S3 settings:

```bash
python3 s3_flood.py --config
```

This will start an interactive configuration wizard where you can set:
- S3 endpoint URLs
- Access key and secret key
- Bucket name
- Parallel threads count
- File group sizes and counts
- Infinite loop settings

## Usage

### Running the Application

After configuration, run the application:

```bash
./run.sh
```

Or directly:

```bash
python3 s3_flood.py
```

### Command Line Options

- `python3 s3_flood.py` - Start the TUI application
- `python3 s3_flood.py --config` - Run the interactive configuration wizard

## Project Structure

```
s3Flood/
├── s3_flood.py              # Main application
├── config.yaml              # Configuration file
├── requirements.txt         # Python dependencies
├── install.sh               # Installation script
├── run.sh                   # Run script
├── README.md                # This file
├── start_minio.sh           # MinIO startup script (for testing)
├── test_s3_connection.py    # S3 connection test
└── demo/                    # Demo scripts
    ├── final_demo.py        # Final demo script
    ├── test_progress.py     # Progress tracking test
    └── demo_progress.py     # Progress demo
```

## Configuration File

The application uses a YAML configuration file (`config.yaml`) with the following options:

```yaml
s3_urls:
  - http://localhost:9000        # S3 endpoint URLs
access_key: minioadmin          # Access key
secret_key: minioadmin          # Secret key
bucket_name: test-bucket        # Bucket name
cluster_mode: false             # Cluster mode (multiple endpoints)
parallel_threads: 5             # Number of parallel threads
file_groups:
  small:
    max_size_mb: 100            # Max size for small files (MB)
    count: 100                  # Number of small files
  medium:
    max_size_mb: 5120           # Max size for medium files (MB)
    count: 50                   # Number of medium files
  large:
    max_size_mb: 20480          # Max size for large files (MB)
    count: 10                   # Number of large files
infinite_loop: true             # Run in infinite loop
cycle_delay_seconds: 15         # Delay between cycles (seconds)
```

## Dependencies

- Python 3.7+
- questionary==2.0.1
- rich==13.7.1
- PyYAML==6.0.1
- s5cmd (https://github.com/peak/s5cmd)

## Development

### Running Tests

```bash
python3 test_s3_connection.py    # Test S3 connection
python3 demo/final_demo.py       # Run demo
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License.

## Support

For issues and feature requests, please open an issue on GitHub.