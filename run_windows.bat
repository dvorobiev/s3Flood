@echo off
chcp 65001 >nul
echo ===============================================
echo S3 Flood - Windows Compatible Version
echo ===============================================
echo.
echo Эта версия полностью совместима с Windows
echo и не требует библиотек rich/questionary.
echo.

REM Setup environment
set PYTHONIOENCODING=utf-8
set TERM=xterm
set COLUMNS=120
set LINES=30

REM Add tools directory to PATH for this session
if exist "tools\s5cmd.exe" (
    echo [INFO] Используется локальный s5cmd из tools/
    set "PATH=%~dp0tools;%PATH%"
)

echo [INFO] Запуск S3 Flood Windows версии...
echo [INFO] Нажмите Ctrl+C для выхода
echo.

python s3_flood_windows.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Ошибка запуска Windows версии!
    echo [INFO] Проверьте установку Python и попробуйте:
    echo   python s3_flood_windows.py
    echo.
    pause
)

echo.
echo [INFO] S3 Flood завершен
pause