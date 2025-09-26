@echo off
echo Installing S3 Flood for Windows...
python --version || (echo ERROR: Python required && pause && exit /b 1)
pip install -r requirements.txt || (echo ERROR: Failed to install dependencies && pause && exit /b 1)
echo Installation complete! Run: python s3_flood.py
pause
