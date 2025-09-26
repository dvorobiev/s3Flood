# S3 Flood Windows Installation Guide

## FULLY AUTOMATED INSTALLATION (Recommended!)

1. Download the project from GitHub:
   - Go to https://github.com/dvorobiev/s3Flood
   - Click "Code" → "Download ZIP"
   - Extract the archive to a convenient folder

2. Run automatic installation:
   - Open the project folder
   - **Right-click** on `install.bat` → "Run as administrator"
   - Wait for installation to complete (may take 5-10 minutes)

**What the script will install:**
- Python 3.11 (automatically)
- All required Python libraries
- s5cmd (S3 command-line tool)

## Running the Application

**Multiple ways to run S3 Flood on Windows:**

1. **run.bat** - Enhanced launcher with Windows Terminal support
2. **run.ps1** - PowerShell version (recommended for console issues)
3. **run_simple.bat** - Fallback version for compatibility issues
4. **Direct command:** `python s3_flood.py`

## Troubleshooting Console Issues

**If you see prompt_toolkit/questionary or rich library errors:**

### SOLUTION 1: Use Simple Compatible Version (Recommended)
```cmd
run_simple.bat
```
This version works on ANY Windows system without console library issues.

### SOLUTION 2: Test Your System Compatibility
```cmd
test_compatibility.bat
```
This will tell you which version to use.

### SOLUTION 3: Try PowerShell
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\run.ps1
```

### SOLUTION 4: Enhanced Launcher
```cmd
run.bat
```
This automatically tries main version, then falls back to simple version.

### Available Launchers:
- **run_simple.bat** - Always works, basic functionality
- **run.bat** - Tries advanced version, falls back to simple
- **run.ps1** - PowerShell version
- **test_compatibility.bat** - Tests what works on your system

## If Problems Occur

1. **Administrator rights error:**
   - You MUST run install.bat as administrator

2. **Download errors:**
   - Check internet connection
   - Temporarily disable antivirus during installation

3. **Manual installation (last resort):**
   ```cmd
   # Install Python from python.org
   pip install -r requirements.txt
   # Download s5cmd from github.com/peak/s5cmd/releases
   ```

## Encoding Fix

The new version of install.bat and run.bat should display correctly in Windows console.
If you still see garbled text, your Windows console might need encoding adjustment.

## Configuration

- Run: `python s3_flood.py --config`
- Or edit the `config.yaml` file manually

