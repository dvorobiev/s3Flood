@echo off
echo Testing Windows Console Compatibility...
echo.

REM Test 1: Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [FAIL] Python not found
    goto :end
) else (
    echo [PASS] Python found
)

REM Test 2: Try rich library
python -c "from rich.console import Console; Console().print(\"Test\")" >nul 2>&1
if %errorlevel% neq 0 (
    echo [FAIL] Rich library has console issues
    echo [INFO] Recommendation: Use run_simple.bat
    goto :end
) else (
    echo [PASS] Rich library works
)

REM Test 3: Try questionary
python -c "import questionary" >nul 2>&1
if %errorlevel% neq 0 (
    echo [FAIL] Questionary not available
    goto :end
) else (
    echo [PASS] Questionary available
)

echo.
echo [SUCCESS] Your system should work with the main version
echo [INFO] Recommendation: Use run.bat

:end
echo.
pause
