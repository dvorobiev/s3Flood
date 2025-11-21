#!/bin/bash
# Скрипт для создания portable дистрибутива Windows
# Запускать на машине с интернетом (macOS/Linux/Windows с WSL)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/windows-portable-build"
DIST_DIR="$BUILD_DIR/s3flood-portable"
PYTHON_VERSION="3.11.9"
PYTHON_EMBED_URL="https://www.python.org/ftp/python/${PYTHON_VERSION}/python-${PYTHON_VERSION}-embed-amd64.zip"

echo "🚀 Сборка portable дистрибутива для Windows"
echo "=========================================="
echo ""

# Создаём директории
rm -rf "$BUILD_DIR"
mkdir -p "$DIST_DIR"

# 1. Скачиваем Python embedded (portable Python)
echo "📥 Скачивание Python ${PYTHON_VERSION} embedded..."
PYTHON_ZIP="$BUILD_DIR/python-embed.zip"
if [ ! -f "$PYTHON_ZIP" ]; then
    curl -L -o "$PYTHON_ZIP" "$PYTHON_EMBED_URL"
fi

# Распаковываем Python
echo "📦 Распаковка Python..."
cd "$DIST_DIR"
unzip -q "$PYTHON_ZIP" || python -m zipfile -e "$PYTHON_ZIP" .

# 2. Настраиваем Python embedded для работы с pip
echo "🔧 Настройка Python embedded..."
# Скачиваем get-pip.py
curl -L -o get-pip.py https://bootstrap.pypa.io/get-pip.py

# Устанавливаем pip в embedded Python
echo "📥 Установка pip..."
python.exe get-pip.py --no-warn-script-location
rm get-pip.py

# 3. Создаём venv и устанавливаем зависимости
echo "📦 Создание виртуального окружения..."
python.exe -m venv venv

echo "📥 Установка зависимостей..."
# Активируем venv (Windows стиль)
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate || . venv/Scripts/activate
else
    # Fallback для Linux/WSL
    source venv/bin/activate
fi

pip install --upgrade pip --quiet
pip install pydantic rich pyyaml --quiet

# Устанавливаем s3flood
echo "📥 Установка s3flood..."
cd "$SCRIPT_DIR"
pip install -e . --quiet

# 4. Копируем необходимые файлы
echo "📋 Копирование файлов..."
cd "$DIST_DIR"
mkdir -p s3flood
cp -r "$SCRIPT_DIR/src/s3flood"/* s3flood/ 2>/dev/null || cp -r "$SCRIPT_DIR/src/s3flood" s3flood/
cp "$SCRIPT_DIR/config.sample.yaml" .
cp "$SCRIPT_DIR/README.md" .

# 5. Создаём batch-скрипты для запуска
echo "📝 Создание скриптов запуска..."

# s3flood.bat - основной скрипт
cat > s3flood.bat << 'BAT_EOF'
@echo off
setlocal

REM Определяем директорию скрипта
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Активируем venv и запускаем s3flood
call venv\Scripts\activate.bat
python -m s3flood %*
BAT_EOF

# s3flood-cmd.bat - для запуска конкретной команды
cat > s3flood-cmd.bat << 'BAT_EOF'
@echo off
setlocal

REM Определяем директорию скрипта
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Активируем venv и запускаем команду
call venv\Scripts\activate.bat
python -m s3flood %*
BAT_EOF

# 6. Создаём инструкцию
cat > INSTALL.txt << 'INSTALL_EOF'
s3flood Portable для Windows
============================

Это portable версия s3flood - не требует установки Python.

Использование:
1. Распакуйте архив в любую папку (например, C:\s3flood-portable)
2. Убедитесь, что AWS CLI установлен и доступен в PATH
3. Запускайте команды через s3flood.bat:

   s3flood.bat dataset-create --path .\loadset --target-bytes 5GB
   s3flood.bat run --profile write-heavy --endpoint http://localhost:9000 --bucket test

Или напрямую через Python:
   venv\Scripts\python.exe -m s3flood --help

Требования:
- Windows 10/11 (64-bit)
- AWS CLI должен быть установлен отдельно

Примечание: Python embedded включён в этот дистрибутив, дополнительная установка не требуется.
INSTALL_EOF

# 7. Упаковываем в ZIP
echo "📦 Создание архива..."
cd "$BUILD_DIR"
ZIP_NAME="s3flood-windows-portable-$(date +%Y%m%d).zip"
zip -r "$ZIP_NAME" s3flood-portable -q

echo ""
echo "✅ Готово!"
echo "📦 Архив создан: $BUILD_DIR/$ZIP_NAME"
echo "📏 Размер: $(du -h "$BUILD_DIR/$ZIP_NAME" | cut -f1)"
echo ""
echo "Скопируйте этот ZIP на офлайн Windows машину и распакуйте."
echo "Запускайте команды через s3flood.bat"

