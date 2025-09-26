@echo off
chcp 65001 >nul
echo ===============================================
echo S3 Flood - S3 Storage Load Testing Tool
echo ===============================================
echo.

REM Check Python installation
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found!
    echo [INFO] Please run install.bat to install Python
    pause
    exit /b 1
)

REM Add local tools directory to PATH
if exist "tools\s5cmd.exe" (
    set "PATH=%~dp0tools;%PATH%"
    echo [INFO] Using local s5cmd from tools/
)

echo [INFO] Starting S3 Flood...
echo [INFO] Press Ctrl+C to exit
echo.
python s3_flood.py
echo.
echo [INFO] S3 Flood has finished
pause
