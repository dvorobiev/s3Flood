@echo off
echo ===============================================
echo S3 Flood - Инструмент нагрузочного тестирования S3
echo ===============================================
echo.

REM Проверка Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ОШИБКА] Python не найден!
    echo [ИНФО] Запустите install.bat для установки
    pause
    exit /b 1
)

REM Добавление локальной папки tools в PATH
if exist "tools\s5cmd.exe" (
    set "PATH=%~dp0tools;%PATH%"
    echo [ИНФО] Используем локальный s5cmd из tools/
)

echo [ИНФО] Запуск S3 Flood...
echo [ИНФО] Нажмите Ctrl+C для выхода
echo.
python s3_flood.py
echo.
echo [ИНФО] S3 Flood завершил работу
pause
