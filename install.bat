@echo off
setlocal EnableDelayedExpansion

echo ===============================================
echo S3 Flood - Автоматическая установка для Windows
echo ===============================================
echo.

REM Проверка прав администратора
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [ОШИБКА] Требуются права администратора!
    echo [ИНФО] Щелкните правой кнопкой на install.bat и выберите "Запуск от имени администратора"
    pause
    exit /b 1
)

echo [ИНФО] Права администратора подтверждены
echo.

REM Проверка наличия Python
echo [1/4] Проверка Python...
python --version >nul 2>&1
if %errorlevel% equ 0 (
    echo [УСПЕХ] Python уже установлен:
    python --version
    goto :install_pip_deps
)

echo [ИНФО] Python не найден. Начинаю автоматическую установку...
echo.

REM Создание временной папки
if not exist "%TEMP%\s3flood_install" mkdir "%TEMP%\s3flood_install"
cd /d "%TEMP%\s3flood_install"

REM Скачивание Python
echo [ИНФО] Скачивание Python 3.11...
powershell -Command "try { Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe' -OutFile 'python-installer.exe' -UseBasicParsing } catch { Write-Host 'Ошибка скачивания Python'; exit 1 }"
if %errorlevel% neq 0 (
    echo [ОШИБКА] Не удалось скачать Python!
    echo [ИНФО] Пожалуйста, установите Python вручную с https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [ИНФО] Установка Python... (это может занять несколько минут)
start /wait python-installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
if %errorlevel% neq 0 (
    echo [ОШИБКА] Установка Python завершилась с ошибкой!
    pause
    exit /b 1
)

echo [УСПЕХ] Python установлен!
echo [ИНФО] Обновление PATH...
refreshenv
set PATH=%PATH%;C:\Program Files\Python311;C:\Program Files\Python311\Scripts

REM Проверка установки Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ПРЕДУПРЕЖДЕНИЕ] Python установлен, но требуется перезагрузка системы
    echo [ИНФО] Перезагрузите компьютер и запустите install.bat снова
    pause
    exit /b 1
)

echo [УСПЕХ] Python готов к использованию!
echo.

:install_pip_deps
echo [2/4] Установка Python зависимостей...
cd /d "%~dp0"
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ОШИБКА] Не удалось установить Python зависимости!
    pause
    exit /b 1
)

echo [УСПЕХ] Python зависимости установлены!
echo.

REM Проверка и установка s5cmd
echo [3/4] Проверка s5cmd...
s5cmd --help >nul 2>&1
if %errorlevel% equ 0 (
    echo [УСПЕХ] s5cmd уже установлен:
    s5cmd --version
    goto :finish
)

echo [ИНФО] s5cmd не найден. Начинаю установку...

REM Создание папки tools
if not exist "tools" mkdir tools

REM Скачивание s5cmd
echo [ИНФО] Скачивание s5cmd для Windows...
powershell -Command "try { $latest = (Invoke-RestMethod 'https://api.github.com/repos/peak/s5cmd/releases/latest').tag_name; $url = \"https://github.com/peak/s5cmd/releases/download/$latest/s5cmd_$($latest.TrimStart('v'))_Windows-64bit.zip\"; Invoke-WebRequest -Uri $url -OutFile 'tools\s5cmd.zip' -UseBasicParsing } catch { Write-Host 'Ошибка скачивания s5cmd'; exit 1 }"
if %errorlevel% neq 0 (
    echo [ОШИБКА] Не удалось скачать s5cmd!
    echo [ИНФО] Пожалуйста, скачайте s5cmd вручную с https://github.com/peak/s5cmd/releases
    goto :finish
)

REM Распаковка s5cmd
echo [ИНФО] Распаковка s5cmd...
powershell -Command "try { Expand-Archive -Path 'tools\s5cmd.zip' -DestinationPath 'tools\' -Force } catch { Write-Host 'Ошибка распаковки s5cmd'; exit 1 }"
if %errorlevel% neq 0 (
    echo [ОШИБКА] Не удалось распаковать s5cmd!
    goto :finish
)

REM Очистка временных файлов
del "tools\s5cmd.zip" >nul 2>&1

REM Добавление s5cmd в PATH для текущей сессии
set "PATH=%~dp0tools;%PATH%"

REM Проверка s5cmd
tools\s5cmd.exe --help >nul 2>&1
if %errorlevel% equ 0 (
    echo [УСПЕХ] s5cmd установлен в папку tools/
    echo [ИНФО] Для постоянного доступа добавьте %~dp0tools в системную переменную PATH
else
    echo [ПРЕДУПРЕЖДЕНИЕ] s5cmd установлен, но может потребоваться ручная настройка
)

echo.

:finish
echo [4/4] Финализация установки...
echo.
echo ===============================================
echo УСТАНОВКА ЗАВЕРШЕНА УСПЕШНО!
echo ===============================================
echo.
echo Установленные компоненты:
echo - Python (с pip)
echo - Все Python зависимости
echo - s5cmd (инструмент для работы с S3)
echo.
echo Для запуска S3 Flood:
echo 1. Дважды щелкните run.bat
echo 2. Или выполните: python s3_flood.py
echo.
echo Настройка:
echo - Запустите: python s3_flood.py --config
echo - Или отредактируйте файл config.yaml
echo.
echo ВАЖНО: Если s5cmd не работает, добавьте папку
echo %~dp0tools в системную переменную PATH
echo.
REM Очистка временных файлов
if exist "%TEMP%\s3flood_install" rmdir /s /q "%TEMP%\s3flood_install" >nul 2>&1

pause
