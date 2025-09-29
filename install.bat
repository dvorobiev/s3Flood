@echo off
chcp 65001 >nul
echo ===============================================
echo S3 Flood - Simple Windows Installation / S3 Flood - Простая установка Windows
echo ===============================================
echo.

REM Check if Python is installed / Проверка установки Python
echo [INFO] Checking Python installation... / [INFO] Проверка установки Python...
python --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python is not installed or not in PATH / [ERROR] Python не установлен или не в PATH
    echo Please install Python 3.7+ from https://python.org / Пожалуйста, установите Python 3.7+ с https://python.org
    echo Make sure to check "Add Python to PATH" during installation / Убедитесь, что отметили "Add Python to PATH" при установке
    pause
    exit /b 1
)

echo [SUCCESS] Python found: / [SUCCESS] Python найден:
python --version
echo.

REM Install required Python packages / Установка необходимых пакетов Python
echo [INFO] Installing required Python packages... / [INFO] Установка необходимых пакетов Python...
python -m pip install --upgrade pip
python -m pip install pyyaml
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install Python packages / [ERROR] Не удалось установить пакеты Python
    pause
    exit /b 1
)
echo [SUCCESS] Python packages installed / [SUCCESS] Пакеты Python установлены
echo.

REM Create tools directory / Создание директории tools
if not exist "tools" mkdir tools

REM Create default config if it doesn't exist / Создание конфигурации по умолчанию, если она не существует
if not exist "config.yaml" (
    echo [INFO] Creating default configuration...
    python -c "import yaml; yaml.dump({'s3_urls': ['http://localhost:9000'], 'access_key': 'minioadmin', 'secret_key': 'minioadmin', 'bucket_name': 'test-bucket', 'cluster_mode': False, 'parallel_threads': 5, 'file_groups': {'small': {'max_size_mb': 100, 'count': 10}, 'medium': {'max_size_mb': 1024, 'count': 5}, 'large': {'max_size_mb': 5120, 'count': 2}}, 'infinite_loop': True, 'cycle_delay_seconds': 15, 'test_files_directory': './s3_temp_files'}, open('config.yaml', 'w'), default_flow_style=False)"
    if %ERRORLEVEL% equ 0 (
        echo [SUCCESS] Default config created / [SUCCESS] Конфигурация по умолчанию создана
    ) else (
        echo [WARNING] Could not create default config / [WARNING] Не удалось создать конфигурацию по умолчанию
    )
)

echo.
echo ===============================================
echo Installation Complete! / Установка завершена!
echo ===============================================
echo.
echo You can now run S3 Flood using: / Теперь вы можете запустить S3 Flood с помощью:
echo   run_simple.bat    - Simple Windows compatible version (RECOMMENDED) / Простая версия совместимая с Windows (РЕКОМЕНДУЕТСЯ)
echo   run.bat           - Full version (may have compatibility issues) / Полная версия (может иметь проблемы совместимости)
echo.
echo [INFO] s5cmd will be downloaded automatically when needed / [INFO] s5cmd будет загружен автоматически при необходимости
echo [INFO] For configuration, edit config.yaml or use the built-in menu / [INFO] Для настройки отредактируйте config.yaml или используйте встроенное меню
echo.
pause
