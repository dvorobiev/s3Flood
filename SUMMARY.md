# S3 Flood - Project Summary

## Overview
S3 Flood is a TUI (Terminal User Interface) application for testing S3-compatible backends using s5cmd. It provides a comprehensive solution for generating load on S3 services with configurable parameters.

## Key Features Implemented

1. **TUI Interface**:
   - Built with [Rich](https://github.com/Textualize/rich) for beautiful terminal output
   - Interactive menus using [Questionary](https://github.com/tmbo/questionary)

2. **Configuration Management**:
   - YAML-based configuration file (config.yaml)
   - Interactive configuration setup
   - Support for single or multiple S3 endpoints (cluster mode)

3. **File Generation**:
   - Three file groups: small (up to 100MB), medium (up to 5GB), large (up to 20GB)
   - Configurable count for each file group
   - Random file sizes within specified ranges

4. **S3 Operations**:
   - Parallel upload/download/delete operations using s5cmd
   - Configurable parallelism (number of threads)
   - Support for cluster mode with multiple endpoints

5. **Test Execution**:
   - Complete cycle: create files → upload → download (to /dev/null) → delete
   - Infinite loop mode with configurable delays
   - Graceful shutdown with Ctrl+C handling

6. **Statistics Tracking**:
   - Files uploaded/downloaded/deleted
   - Operation timing statistics
   - Average times per operation

## Files Created

- `s3_flood.py`: Main application code
- `config.yaml`: Default configuration file
- `requirements.txt`: Python dependencies
- `README.md`: Documentation and usage instructions
- `install.sh`: Installation script
- `run.sh`: Execution script
- `SUMMARY.md`: This summary file

## Dependencies

- Python 3.7+
- s5cmd (S3 command-line tool)
- Python packages: questionary, rich, PyYAML

## Usage

1. Install dependencies:
   ```bash
   ./install.sh
   ```

2. Run the application:
   ```bash
   ./run.sh
   ```

3. Or run directly:
   ```bash
   python s3_flood.py
   ```

## Architecture

The application follows a modular design:

1. **S3FloodTester Class**: Main class handling all operations
2. **Configuration Management**: Load/save YAML config files
3. **File Generation**: Create test files of various sizes
4. **S3 Operations**: Upload/download/delete using s5cmd
5. **Statistics**: Track and display performance metrics
6. **UI Layer**: TUI with menus and progress indicators

## Extensibility

The code is structured to allow easy extensions:
- Add new file size categories
- Implement different test scenarios
- Add more detailed statistics
- Extend cluster mode functionality
- Add support for different S3 operations