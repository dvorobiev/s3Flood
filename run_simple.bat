@echo off
chcp 65001 >nul 2>&1
echo S3 Flood - Simple Windows Compatible Version
echo =============================================
echo.
echo This version works on older Windows systems
echo that have issues with rich/questionary libraries.
echo.

REM Check Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found!
    echo [INFO] Please run install.bat first
    pause
    exit /b 1
)

REM Add tools to PATH
if exist "tools\s5cmd.exe" (
    set "PATH=%~dp0tools;%PATH%"
    echo [INFO] Using local s5cmd from tools/
)

REM Set compatible environment
set PYTHONIOENCODING=utf-8
set TERM=dumb
set FORCE_COLOR=0

echo [INFO] Starting S3 Flood Simple Version...
echo.

REM Run simple version
python s3_flood_simple.py

echo.
echo [INFO] S3 Flood Simple has finished
pause
