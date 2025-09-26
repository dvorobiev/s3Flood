@echo off
chcp 65001 >nul 2>&1
echo S3 Flood - Windows Edition
echo ===========================
echo.

REM Setup environment
set PYTHONIOENCODING=utf-8
set TERM=xterm

REM Add tools directory to PATH for this session
if exist "tools\s5cmd.exe" (
    echo [INFO] Using local s5cmd from tools/
    set "PATH=%~dp0tools;%PATH%"
)

echo [INFO] Starting S3 Flood...
echo [INFO] Press Ctrl+C to exit
echo.

python s3_flood_windows_final.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Failed to start S3 Flood!
    echo [INFO] Please check your Python installation.
    echo.
    pause
)

echo.
echo [INFO] S3 Flood completed
pause