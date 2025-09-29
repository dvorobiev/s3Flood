@echo off
chcp 65001 >nul 2>&1
echo S3 Flood - Windows Edition / S3 Flood - Версия для Windows
echo ===========================
echo.

REM Setup environment / Настройка окружения
set PYTHONIOENCODING=utf-8
set TERM=xterm

REM Add tools directory to PATH for this session / Добавление директории tools в PATH для этой сессии
if exist "tools\s5cmd.exe" (
    echo [INFO] Using local s5cmd from tools/ / [INFO] Использование локального s5cmd из tools/
    set "PATH=%~dp0tools;%PATH%"
)

echo [INFO] Starting S3 Flood... / [INFO] Запуск S3 Flood...
echo [INFO] Press Ctrl+C to exit / [INFO] Нажмите Ctrl+C для выхода
echo.

python s3_flood_windows.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Failed to start S3 Flood! / [ERROR] Не удалось запустить S3 Flood!
    echo [INFO] Please check your Python installation. / [INFO] Пожалуйста, проверьте установку Python.
    echo.
    pause
)

echo.
echo [INFO] S3 Flood completed / [INFO] S3 Flood завершен
pause