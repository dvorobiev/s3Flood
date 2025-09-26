@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

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

REM Set environment variables for better Windows console support
set PYTHONIOENCODING=utf-8
set COLUMNS=120
set LINES=30

echo [INFO] Starting S3 Flood...
echo [INFO] Press Ctrl+C to exit
echo [INFO] If you see console errors, try running in Windows Terminal or PowerShell
echo.

REM Try to run with Windows Terminal if available, otherwise use regular console
where wt >nul 2>&1
if %errorlevel% equ 0 (
    echo [INFO] Windows Terminal detected, using improved console...
    wt -w 0 cmd /c "cd /d "%~dp0" && python s3_flood_windows.py && pause"
) else (
    echo [INFO] Trying Windows-compatible version...
    python s3_flood_windows.py
    if %errorlevel% neq 0 (
        echo.
        echo [WARNING] Windows version failed, trying main version...
        echo [INFO] This may have library compatibility issues...
        echo.
        python s3_flood.py
    )
)

echo.
echo [INFO] S3 Flood has finished
pause
