# S3 Flood - PowerShell Edition / S3 Flood - PowerShell версия

![Version](https://img.shields.io/badge/version-1.6.3-blue.svg) ![License](https://img.shields.io/badge/license-MIT-green.svg) ![Platform](https://img.shields.io/badge/platform-Windows%20PowerShell-blue.svg) ![PowerShell](https://img.shields.io/badge/powershell-5.1%2B-blue.svg)

English version / Русская версия

A PowerShell implementation of the S3 Flood tool for testing S3-compatible storage performance with parallel upload/download operations.

PowerShell реализация инструмента S3 Flood для тестирования производительности S3-совместимых хранилищ с параллельными операциями загрузки/скачивания.

## 📋 Features / Возможности

**English:**
- **Parallel Operations**: Upload and download files concurrently using PowerShell jobs
- **Batch Processing**: Process files in configurable batches (default: 10 files per batch)
- **Mixed File Sizes**: Generates test files of various sizes (small, medium, large, huge)
- **Automatic Cleanup**: Cleans up temporary files and S3 objects after each cycle
- **Real-time Logging**: Color-coded console output with timestamps
- **Graceful Shutdown**: Properly handles Ctrl+C interruption
- **Algorithm Selection**: Choose between different testing algorithms:
  - *Traditional (Write-Read-Delete)*: Upload files → Read files → Delete files
  - *Infinite Write*: Continuously upload files without deletion

**Русский:**
- **Параллельные операции**: Загрузка и скачивание файлов одновременно с использованием PowerShell jobs
- **Пакетная обработка**: Обработка файлов в настраиваемых пакетах (по умолчанию: 10 файлов за пакет)
- **Смешанные размеры файлов**: Генерирует тестовые файлы различных размеров (маленькие, средние, большие, огромные)
- **Автоматическая очистка**: Очищает временные файлы и объекты S3 после каждого цикла
- **Логирование в реальном времени**: Цветной вывод в консоль с временными метками
- **Корректная остановка**: Правильно обрабатывает прерывание через Ctrl+C
- **Выбор алгоритма**: Выбор между различными алгоритмами тестирования:
  - *Традиционный (Запись-Чтение-Удаление)*: Загрузка файлов → Чтение файлов → Удаление файлов
  - *Бесконечная запись*: Непрерывная загрузка файлов без удаления

## 📁 File Structure / Структура файлов

```
s3Flood/
├── s3_flood_powershell.ps1     # Main PowerShell script / Основной PowerShell скрипт
├── README_POWERSHELL.md        # This file / Этот файл
└── rclone                      # Required rclone binary (must be in same directory) / Требуемый бинарный файл rclone (должен быть в той же директории)
```

## ⚙️ Requirements / Требования

**English:**
1. **Windows PowerShell 5.1** or **PowerShell 7+**
2. **rclone** binary in the same directory as the script
3. **S3-compatible storage** with configured rclone remote

**Русский:**
1. **Windows PowerShell 5.1** или **PowerShell 7+**
2. Бинарный файл **rclone** в той же директории, что и скрипт
3. **S3-совместимое хранилище** с настроенным rclone remote

## 🚀 Quick Start / Быстрый старт

**English:**
1. **Download rclone**:
   - Download rclone for Windows from [https://rclone.org/downloads/](https://rclone.org/downloads/)
   - Extract `rclone.exe` to the same directory as the script

2. **Configure rclone**:
   ```bash
   ./rclone config
   ```
   Create a remote named `demo` (or change `$rcloneRemote` in the script)

3. **Run the script**:
   ```powershell
   .\s3_flood_powershell.ps1
   ```

**Русский:**
1. **Скачайте rclone**:
   - Скачайте rclone для Windows с [https://rclone.org/downloads/](https://rclone.org/downloads/)
   - Распакуйте `rclone.exe` в ту же директорию, что и скрипт

2. **Настройте rclone**:
   ```bash
   ./rclone config
   ```
   Создайте remote с именем `demo` (или измените `$rcloneRemote` в скрипте)

3. **Запустите скрипт**:
   ```powershell
   .\s3_flood_powershell.ps1
   ```

## 🛠️ Configuration / Настройка

**English:**
Edit these variables at the top of [s3_flood_powershell.ps1](file:///Users/dvorobiev/s3Flood/s3_flood_powershell.ps1):

```powershell
# Configuration
$rcloneRemote = "demo"        # Your rclone remote name
$bucketName = "backup"        # S3 bucket name
$localTempDir = ".\S3_TEMP_FILES"  # Local temp directory
$batchSize = 10               # Files per batch
```

**Русский:**
Измените эти переменные в начале файла [s3_flood_powershell.ps1](file:///Users/dvorobiev/s3Flood/s3_flood_powershell.ps1):

```powershell
# Настройка
$rcloneRemote = "demo"        # Имя вашего rclone remote
$bucketName = "backup"        # Имя S3 бакета
$localTempDir = ".\S3_TEMP_FILES"  # Локальная временная директория
$batchSize = 10               # Файлов в пакете
```

## 📊 Test File Generation / Генерация тестовых файлов

**English:**
The script automatically generates 100 test files:

- **30 Small files**: 1MB - 100MB
- **30 Medium files**: 101MB - 1024MB
- **30 Large files**: 1GB - 10GB
- **10 Huge files**: 11GB - 100GB

**Русский:**
Скрипт автоматически генерирует 100 тестовых файлов:

- **30 Маленьких файлов**: 1MB - 100MB
- **30 Средних файлов**: 101MB - 1024MB
- **30 Больших файлов**: 1GB - 10GB
- **10 Огромных файлов**: 11GB - 100GB

## 🔁 Algorithm / Алгоритм

**English:**
Version 1.7.0 introduces algorithm selection with two options:

1. **Traditional Algorithm (Default)**:
   - Generate Test Files: Create 100 random-sized files in temp directory
   - Batch Upload: Upload files in batches of `$batchSize`
   - Batch Download: Download the same files in batches
   - Delete All: Remove all files from S3 bucket
   - Repeat: Wait 15 seconds and start over

2. **Infinite Write Algorithm**:
   - Generate Test Files: Create 100 random-sized files in temp directory
   - Upload All Files: Upload all files to S3 bucket
   - Continuous Write: Repeatedly re-upload files without deletion
   - Repeat: Generate new files and start over

**Русский:**
В версии 1.7.0 появился выбор алгоритма с двумя вариантами:

1. **Традиционный алгоритм (по умолчанию)**:
   - Генерация тестовых файлов: Создание 100 файлов случайных размеров во временной директории
   - Пакетная загрузка: Загрузка файлов пакетами по `$batchSize`
   - Пакетное скачивание: Скачивание тех же файлов пакетами
   - Удалить все: Удаление всех файлов из S3 бакета
   - Повтор: Ожидание 15 секунд и начало заново

2. **Алгоритм бесконечной записи**:
   - Генерация тестовых файлов: Создание 100 файлов случайных размеров во временной директории
   - Загрузка всех файлов: Загрузка всех файлов в S3 бакет
   - Непрерывная запись: Повторная загрузка файлов без удаления
   - Повтор: Генерация новых файлов и начало заново

## 🎨 Console Colors / Цвета консоли

**English:**
- **Gray**: General information
- **Green**: Success messages
- **Yellow**: Warnings and deletion operations
- **Red**: Errors
- **Cyan**: Batch processing information
- **Magenta**: Cycle completion

**Русский:**
- **Серый**: Общая информация
- **Зеленый**: Сообщения об успехе
- **Желтый**: Предупреждения и операции удаления
- **Красный**: Ошибки
- **Голубой**: Информация о пакетной обработке
- **Пурпурный**: Завершение цикла

## ⚠️ Important Notes / Важные замечания

**English:**
- The script will **delete all files** from the specified bucket during testing
- Make sure to use a **dedicated test bucket**
- Large files (1GB+) may take significant time to generate
- The script runs indefinitely until stopped with Ctrl+C
- Requires administrator privileges to create large files with `fsutil`

**Русский:**
- Скрипт будет **удалять все файлы** из указанного бакета во время тестирования
- Убедитесь, что используете **выделенный тестовый бакет**
- Большие файлы (1GB+) могут требовать значительного времени для генерации
- Скрипт выполняется бесконечно до остановки через Ctrl+C
- Требуются права администратора для создания больших файлов с помощью `fsutil`

## 🛑 Stopping the Script / Остановка скрипта

**English:**
Press `Ctrl+C` to gracefully stop the script. It will:
- Cancel all running jobs
- Delete temporary files
- Clean up S3 objects
- Exit cleanly

**Русский:**
Нажмите `Ctrl+C` для корректной остановки скрипта. Он:
- Отменит все запущенные задачи
- Удалит временные файлы
- Очистит объекты S3
- Завершится корректно

## 📈 Monitoring / Мониторинг

**English:**
The script provides real-time feedback:
- Batch progress
- File counts
- Operation status
- Timing information