@echo off
REM Disable output redirection to avoid "The system cannot write to the specified device" errors
set /p DUMMY="" <nul >nul 2>&1

REM Basic console setup
chcp 65001 >nul 2>&1
echo S3 Flood - Windows Compatible Version
echo ===============================================
echo.
echo This version is fully compatible with Windows
echo and does not require rich/questionary libraries.
echo.

REM Setup environment
set PYTHONIOENCODING=utf-8
set TERM=xterm
set COLUMNS=120
set LINES=30

REM Add tools directory to PATH for this session
if exist "tools\s5cmd.exe" (
    echo [INFO] Using local s5cmd from tools/
    set "PATH=%~dp0tools;%PATH%"
)

echo [INFO] Starting S3 Flood Windows version...
echo [INFO] Press Ctrl+C to exit
echo.

REM Try ultra-safe version first
python s3_flood_ultra_safe.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo [WARNING] Ultra-safe version failed, trying original Windows version...
    echo.
    python s3_flood_windows.py
    if %ERRORLEVEL% neq 0 (
        echo.
        echo [ERROR] Both Windows versions failed!
        echo [INFO] Please check your Python installation and try:
        echo   python s3_flood_ultra_safe.py
        echo.
        pause
    )
)

echo.
echo [INFO] S3 Flood completed
REM Don't pause automatically to avoid console issues
echo Press any key to continue...
pause >nul