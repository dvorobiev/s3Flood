@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

echo ===============================================
echo S3 Flood - Automatic Windows Installation
echo ===============================================
echo.

REM Check administrator privileges
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [ERROR] Administrator privileges required!
    echo [INFO] Right-click install.bat and select "Run as administrator"
    pause
    exit /b 1
)

echo [INFO] Administrator privileges confirmed
echo.

REM Check Python installation
echo [1/4] Checking Python...
python --version >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] Python already installed:
    python --version
    goto :install_pip_deps
)

echo [INFO] Python not found. Starting automatic installation...
echo.

REM Create temporary directory
if not exist "%TEMP%\s3flood_install" mkdir "%TEMP%\s3flood_install"
cd /d "%TEMP%\s3flood_install"

REM Download Python
echo [INFO] Downloading Python 3.11...
powershell -Command "try { Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe' -OutFile 'python-installer.exe' -UseBasicParsing } catch { Write-Host 'Error downloading Python'; exit 1 }"
if %errorlevel% neq 0 (
    echo [ERROR] Failed to download Python!
    echo [INFO] Please install Python manually from https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [INFO] Installing Python... (this may take several minutes)
start /wait python-installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
if %errorlevel% neq 0 (
    echo [ERROR] Python installation failed!
    pause
    exit /b 1
)

echo [SUCCESS] Python installed!
echo [INFO] Updating PATH...
refreshenv
set PATH=%PATH%;C:\Program Files\Python311;C:\Program Files\Python311\Scripts

REM Check Python installation
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARNING] Python installed but system restart required
    echo [INFO] Please restart computer and run install.bat again
    pause
    exit /b 1
)

echo [SUCCESS] Python ready to use!
echo.

:install_pip_deps
echo [2/4] Installing Python dependencies...
cd /d "%~dp0"
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ERROR] Failed to install Python dependencies!
    pause
    exit /b 1
)

echo [SUCCESS] Python dependencies installed!
echo.

REM Check and install s5cmd
echo [3/4] Checking s5cmd...
s5cmd --help >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] s5cmd already installed:
    s5cmd --version
    goto :finish
)

echo [INFO] s5cmd not found. Starting installation...

REM Create tools directory
if not exist "tools" mkdir tools

REM Download s5cmd
echo [INFO] Downloading s5cmd for Windows...
powershell -Command "try { $latest = (Invoke-RestMethod 'https://api.github.com/repos/peak/s5cmd/releases/latest').tag_name; $url = \"https://github.com/peak/s5cmd/releases/download/$latest/s5cmd_$($latest.TrimStart('v'))_Windows-64bit.zip\"; Invoke-WebRequest -Uri $url -OutFile 'tools\s5cmd.zip' -UseBasicParsing } catch { Write-Host 'Error downloading s5cmd'; exit 1 }"
if %errorlevel% neq 0 (
    echo [ERROR] Failed to download s5cmd!
    echo [INFO] Please download s5cmd manually from https://github.com/peak/s5cmd/releases
    goto :finish
)

REM Extract s5cmd
echo [INFO] Extracting s5cmd...
powershell -Command "try { Expand-Archive -Path 'tools\s5cmd.zip' -DestinationPath 'tools\' -Force } catch { Write-Host 'Error extracting s5cmd'; exit 1 }"
if %errorlevel% neq 0 (
    echo [ERROR] Failed to extract s5cmd!
    goto :finish
)

REM Cleanup temporary files
del "tools\s5cmd.zip" >nul 2>&1

REM Add s5cmd to PATH for current session
set "PATH=%~dp0tools;%PATH%"

REM Test s5cmd
tools\s5cmd.exe --help >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] s5cmd installed in tools/ directory
    echo [INFO] To use s5cmd permanently, add %~dp0tools to system PATH
else
    echo [WARNING] s5cmd installed but may need manual configuration
)

echo.

:finish
echo [4/4] Finalizing installation...
echo.
echo ===============================================
echo INSTALLATION COMPLETED SUCCESSFULLY!
echo ===============================================
echo.
echo Installed components:
echo - Python (with pip)
echo - All Python dependencies
echo - s5cmd (S3 command-line tool)
echo.
echo To start S3 Flood:
echo 1. Double-click run.bat
echo 2. Or execute: python s3_flood.py
echo.
echo Configuration:
echo - Run: python s3_flood.py --config
echo - Or edit config.yaml file
echo.
echo IMPORTANT: If s5cmd doesn't work, add this folder
echo %~dp0tools to your system PATH variable
echo.
REM Cleanup temporary files
if exist "%TEMP%\s3flood_install" rmdir /s /q "%TEMP%\s3flood_install" >nul 2>&1

pause
