# S3 Flood - S3 Load Testing Tool
# S3 Flood - Инструмент нагрузочного тестирования S3

![Version](https://img.shields.io/badge/version-1.6.1-blue.svg) ![License](https://img.shields.io/badge/license-MIT-green.svg) ![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS%20%7C%20PowerShell-blue.svg) ![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)

**English:** S3 Flood is a cross-platform terminal interface (TUI) application designed for stress-testing S3-compatible storage systems such as MinIO or AWS S3. It evaluates performance by generating test files and performing parallel upload, download, and delete operations.

**Русский:** S3 Flood - это кроссплатформенное приложение с терминальным интерфейсом (TUI), предназначенное для стресс-тестирования S3-совместимых хранилищ, таких как MinIO или AWS S3. Оно позволяет оценить производительность, генерируя тестовые файлы и выполняя параллельные операции загрузки, скачивания и удаления.

## Supported Platforms / Поддерживаемые платформы

**English:**
- **Linux** (primary platform)
- **Windows** (supported since version 1.0.0)
- **macOS** (basic functionality support)
- **PowerShell** (alternative version for Windows)

**Русский:**
- **Linux** (основная платформа)
- **Windows** (поддержка с версии 1.0.0)
- **macOS** (поддержка базовой функциональности)
- **PowerShell** (альтернативная версия для Windows)

## Features / Возможности

**English:**
- **Interactive TUI**: Easy setup and real-time monitoring
- **Customizable File Generation**: Creation of test files of various sizes (small, medium, large)
- **Parallel Operations**: Concurrent execution of S3 operations using s5cmd
- **File Progress Tracking**: Individual progress indicators for each operation
- **Randomized Load**: Random parallel read/write operations
- **Statistics Collection**: Performance metrics including throughput and operation times
- **Infinite Loop Mode**: Ability to run continuous stress testing

**Русский:**
- **Интерактивный TUI**: Простая настройка и мониторинг в реальном времени
- **Настраиваемая генерация файлов**: Создание тестовых файлов разных размеров (маленькие, средние, большие)
- **Параллельные операции**: Выполнение S3-операций параллельно с использованием s5cmd
- **Отслеживание прогресса по файлам**: Индивидуальные индикаторы прогресса для каждой операции
- **Рандомизированная нагрузка**: Случайные параллельные операции чтения/записи
- **Сбор статистики**: Метрики производительности, включая пропускную способность и время операций
- **Режим бесконечного цикла**: Возможность непрерывного стресс-тестирования

## Requirements / Требования

**English:**
- Python 3.7+
- Installed s5cmd and available in PATH

**Русский:**
- Python 3.7+
- Установленный s5cmd и доступный в PATH

## Installation / Установка

### Linux/macOS

**English:**
1. Clone or download this repository

2. Run the installation script:
   ```bash
   ./install.sh
   ```

**Русский:**
1. Клонируйте или скачайте этот репозиторий

2. Запустите скрипт установки:
   ```bash
   ./install.sh
   ```

### Windows

**English:**
1. **Windows Requirements:**
   - Python 3.7+ (download from [python.org](https://www.python.org/downloads/))
   - When installing Python, be sure to check "Add Python to PATH"

2. **Automatic Installation:**
   - Download the project from GitHub
   - Run `install.bat` as administrator
   - The script will automatically install all dependencies

3. **Manual Windows Installation:**
   ```cmd
   # Install Python dependencies
   pip install -r requirements.txt
   
   # Download s5cmd for Windows
   mkdir tools
   # Download s5cmd from https://github.com/peak/s5cmd/releases
   # Extract s5cmd.exe to the tools/ folder
   ```

**Русский:**
1. **Требования для Windows:**
   - Python 3.7+ (скачать с [python.org](https://www.python.org/downloads/))
   - При установке Python обязательно отметьте "Add Python to PATH"

2. **Автоматическая установка:**
   - Скачайте проект с GitHub
   - Запустите `install.bat` от имени администратора
   - Скрипт автоматически установит все зависимости

3. **Ручная установка для Windows:**
   ```cmd
   # Установка Python зависимостей
   pip install -r requirements.txt
   
   # Скачивание s5cmd для Windows
   mkdir tools
   # Скачайте s5cmd с https://github.com/peak/s5cmd/releases
   # Распакуйте s5cmd.exe в папку tools/
   ```

### Universal Installation / Универсальная установка

**English:**
This script will perform:
- Python 3 and pip3 check
- s5cmd installation
- Python virtual environment creation
- Installation of Python dependencies from requirements.txt

3. Alternatively, you can install dependencies manually:
   ```bash
   # Install s5cmd
   wget -O s5cmd.tar.gz https://github.com/peak/s5cmd/releases/latest/download/s5cmd_$(uname -s)_$(uname -m).tar.gz
   tar -xzf s5cmd.tar.gz
   sudo install s5cmd /usr/local/bin/
   rm s5cmd.tar.gz

   # Create virtual environment
   python3 -m venv venv
   source venv/bin/activate

   # Install Python dependencies
   pip install -r requirements.txt
   ```

**Русский:**
Этот скрипт выполнит:
- Проверку Python 3 и pip3
- Установку s5cmd
- Создание виртуального окружения Python
- Установку зависимостей Python из requirements.txt

3. Альтернативно, вы можете установить зависимости вручную:
   ```bash
   # Установка s5cmd
   wget -O s5cmd.tar.gz https://github.com/peak/s5cmd/releases/latest/download/s5cmd_$(uname -s)_$(uname -m).tar.gz
   tar -xzf s5cmd.tar.gz
   sudo install s5cmd /usr/local/bin/
   rm s5cmd.tar.gz

   # Создание виртуального окружения
   python3 -m venv venv
   source venv/bin/activate

   # Установка зависимостей Python
   pip install -r requirements.txt
   ```

## Configuration / Настройка

**English:**
Before running the application, configure it with your S3 parameters:

```bash
source venv/bin/activate
python s3_flood.py --config
```

This will launch an interactive setup wizard where you can specify:
- S3 endpoint URLs
- Access key and secret key
- Bucket name
- Number of parallel threads
- File group sizes and counts
- Infinite loop settings

**Русский:**
Перед запуском приложения настройте его с вашими параметрами S3:

```bash
source venv/bin/activate
python s3_flood.py --config
```

Это запустит интерактивный мастер настройки, где вы сможете задать:
- URL endpoint'ов S3
- Ключ доступа и секретный ключ
- Имя бакета
- Количество параллельных потоков
- Размеры и количество файлов в группах
- Настройки бесконечного цикла

## Usage / Использование

### Running the Application / Запуск приложения

**English:**
**Linux/macOS:**
```bash
./run.sh
```

Or directly:
```bash
source venv/bin/activate
python s3_flood.py
```

**Windows (Python version):**
```cmd
run.bat
```

Or directly:
```cmd
python s3_flood.py
```

**Windows (PowerShell version):**
```powershell
.\s3_flood_powershell.ps1
```

### Command Line Parameters / Параметры командной строки

**English:**
- `python s3_flood.py` - Launch TUI application
- `python s3_flood.py --config` - Launch interactive setup wizard

For the PowerShell version, parameters are configured in the script itself.

**Русский:**
**Linux/macOS:**
```bash
./run.sh
```

Или напрямую:
```bash
source venv/bin/activate
python s3_flood.py
```

**Windows (Python версия):**
```cmd
run.bat
```

Или напрямую:
```cmd
python s3_flood.py
```

**Windows (PowerShell версия):**
```powershell
.\s3_flood_powershell.ps1
```

### Параметры командной строки

- `python s3_flood.py` - Запуск TUI-приложения
- `python s3_flood.py --config` - Запуск интерактивного мастера настройки

Для PowerShell версии параметры настраиваются в самом скрипте.

## Project Structure / Структура проекта

```
s3Flood/
├── s3_flood.py              # Main application / Основное приложение
├── s3_flood_powershell.ps1  # PowerShell alternative version / Альтернативная версия на PowerShell
├── config.yaml              # Configuration file / Файл конфигурации
├── requirements.txt         # Python dependencies / Зависимости Python
├── install.sh               # Installation script / Скрипт установки
├── run.sh                   # Launch script / Скрипт запуска
├── README.md                # This file / Этот файл
├── README_POWERSHELL.md     # PowerShell version documentation / Документация PowerShell версии
├── start_minio.sh           # MinIO launch script (for testing) / Скрипт запуска MinIO (для тестирования)
├── test_s3_connection.py    # S3 connection test / Тест подключения к S3
└── demo/                    # Demo scripts / Демо-скрипты
    ├── final_demo.py        # Final demo script / Финальный демо-скрипт
    ├── test_progress.py     # Progress tracking test / Тест отслеживания прогресса
    └── demo_progress.py     # Progress demonstration / Демонстрация прогресса
```

## Configuration File / Файл конфигурации

**English:**
The application uses a YAML configuration file ([config.yaml](file:///Users/dvorobiev/s3Flood/config.yaml)) with the following parameters:

```yaml
s3_urls:
  - http://localhost:9000        # S3 endpoint URLs
access_key: minioadmin          # Access key
secret_key: minioadmin          # Secret key
bucket_name: test-bucket        # Bucket name
cluster_mode: false             # Cluster mode (multiple endpoints)
parallel_threads: 5             # Number of parallel threads
file_groups:
  small:
    max_size_mb: 100            # Max size of small files (MB)
    count: 100                  # Number of small files
  medium:
    max_size_mb: 5120           # Max size of medium files (MB)
    count: 50                   # Number of medium files
  large:
    max_size_mb: 20480          # Max size of large files (MB)
    count: 10                   # Number of large files
infinite_loop: true             # Run in infinite loop
cycle_delay_seconds: 15         # Delay between cycles (in seconds)
```

**Русский:**
Приложение использует YAML-файл конфигурации ([config.yaml](file:///Users/dvorobiev/s3Flood/config.yaml)) со следующими параметрами:

```yaml
s3_urls:
  - http://localhost:9000        # URL endpoint'ов S3
access_key: minioadmin          # Ключ доступа
secret_key: minioadmin          # Секретный ключ
bucket_name: test-bucket        # Имя бакета
cluster_mode: false             # Режим кластера (несколько endpoint'ов)
parallel_threads: 5             # Количество параллельных потоков
file_groups:
  small:
    max_size_mb: 100            # Макс. размер маленьких файлов (МБ)
    count: 100                  # Количество маленьких файлов
  medium:
    max_size_mb: 5120           # Макс. размер средних файлов (МБ)
    count: 50                   # Количество средних файлов
  large:
    max_size_mb: 20480          # Макс. размер больших файлов (МБ)
    count: 10                   # Количество больших файлов
infinite_loop: true             # Запуск в бесконечном цикле
cycle_delay_seconds: 15         # Задержка между циклами (в секундах)
```

## Dependencies / Зависимости

**English:**
- Python 3.7+
- questionary==2.0.1
- rich==13.7.1
- PyYAML==6.0.1
- s5cmd (https://github.com/peak/s5cmd)

**Русский:**
- Python 3.7+
- questionary==2.0.1
- rich==13.7.1
- PyYAML==6.0.1
- s5cmd (https://github.com/peak/s5cmd)

## Development / Разработка

### Running Tests / Запуск тестов

**English:**
```bash
source venv/bin/activate
python test_s3_connection.py    # S3 connection test
python demo/final_demo.py       # Run demo
```

**Русский:**
```bash
source venv/bin/activate
python test_s3_connection.py    # Тест подключения к S3
python demo/final_demo.py       # Запуск демо
```

## Contributing / Участие в разработке

**English:**
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push the branch
5. Create a Pull Request

**Русский:**
1. Сделайте форк репозитория
2. Создайте ветку с новой функцией
3. Зафиксируйте изменения
4. Отправьте ветку
5. Создайте Pull Request

## License / Лицензия

**English:**
This project is licensed under the MIT License.

**Русский:**
Этот проект лицензирован по лицензии MIT.

## Support / Поддержка

**English:**
For questions and suggestions regarding functionality, please open an issue on GitHub.

**Русский:**
По вопросам и предложениям по функциональности, пожалуйста, откройте issue на GitHub.