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

- Double-click `run.bat`
- Or open command prompt and execute: `python s3_flood.py`

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

