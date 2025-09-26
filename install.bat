@echo off
chcp 65001 >nul
echo ===============================================
echo S3 Flood - Simple Windows Installation
echo ===============================================
echo.

REM Check if Python is installed
echo [INFO] Checking Python installation...
python --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python 3.7+ from https://python.org
    echo Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)

echo [SUCCESS] Python found:
python --version
echo.

REM Install required Python packages
echo [INFO] Installing required Python packages...
python -m pip install --upgrade pip
python -m pip install pyyaml
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install Python packages
    pause
    exit /b 1
)
echo [SUCCESS] Python packages installed
echo.

REM Create tools directory
if not exist "tools" mkdir tools

REM Create default config if it doesn't exist
if not exist "config.yaml" (
    echo [INFO] Creating default configuration...
    python -c "import yaml; yaml.dump({'s3_urls': ['http://localhost:9000'], 'access_key': 'minioadmin', 'secret_key': 'minioadmin', 'bucket_name': 'test-bucket', 'cluster_mode': False, 'parallel_threads': 5, 'file_groups': {'small': {'max_size_mb': 100, 'count': 10}, 'medium': {'max_size_mb': 1024, 'count': 5}, 'large': {'max_size_mb': 5120, 'count': 2}}, 'infinite_loop': True, 'cycle_delay_seconds': 15, 'test_files_directory': './s3_temp_files'}, open('config.yaml', 'w'), default_flow_style=False)"
    if %ERRORLEVEL% equ 0 (
        echo [SUCCESS] Default config created
    ) else (
        echo [WARNING] Could not create default config
    )
)

echo.
echo ===============================================
echo Installation Complete!
echo ===============================================
echo.
echo You can now run S3 Flood using:
echo   run_simple.bat    - Simple Windows compatible version (RECOMMENDED)
echo   run.bat           - Full version (may have compatibility issues)
echo.
echo [INFO] s5cmd will be downloaded automatically when needed
echo [INFO] For configuration, edit config.yaml or use the built-in menu
echo.
pause
