## s3flood — минималистичный клиент для нагрузочного тестирования S3

Лёгкий консольный инструмент для генерации датасета и прогона профилей нагрузки к S3-совместимому бэкенду. Показывает прозрачный прогресс, пишет метрики в CSV и итоговый отчёт в JSON.

Сейчас реализован рабочий прототип:
- генерация датасета на локальном диске с использованием сидов и симлинков;
- профиль записи (upload) через AWS CLI с автоматической фазой чтения;
- многопоточность выполнения;
- лаконичный прогресс и метрики (CSV/JSON) для записи и чтения.

Планируемая архитектура и расширения описаны в ROADMAP.md.

### Установка

#### Быстрая установка (рекомендуется)

Для Mac и Linux используйте скрипт установки:

```bash
git clone https://github.com/dvorobiev/s3Flood.git
cd s3Flood
./install.sh
```

Скрипт автоматически:
- Проверит наличие Python 3.10+ и AWS CLI
- Создаст виртуальное окружение `.venv`
- Установит все зависимости
- Создаст wrapper скрипт `./s3flood` для удобного запуска

После установки используйте:
```bash
./s3flood dataset-create --path ./loadset --use-symlinks
./s3flood run --profile write-heavy --endpoint http://localhost:9000 --bucket test-bucket
```

#### Ручная установка

Требования:
- Python 3.10+
- AWS CLI в `PATH`

```bash
python3 -m venv .venv
source .venv/bin/activate  # На Windows: .venv\Scripts\activate
pip install -e .
```

#### Windows без доступа к интернету

**Способ 1: Portable дистрибутив (рекомендуется, самый простой)**

**Автоматическая сборка через GitHub Actions:**
- При создании тега `v*` или вручную через Actions → `windows-portable`
- Автоматически создаётся `s3flood-windows-portable.zip` и прикрепляется к релизу
- Скачайте ZIP из последнего релиза на GitHub

**Или локальная сборка:**
На машине с интернетом (macOS/Linux/Windows с WSL) запустите:

1. **Соберите колёса зависимостей и самого проекта**:
   ```bash
   cd s3Flood
   python3 -m venv .venv && source .venv/bin/activate
   pip install --upgrade pip wheel
   pip download --only-binary=:all: --platform win_amd64 --python-version 311 --dest wheels -r requirements.txt
   pip download --only-binary=:all: --platform win_amd64 --python-version 311 --dest wheels .
   ```
   В папке `wheels/` будут все нужные `.whl` файлы (включая `s3flood-*.whl`).

2. **Скопируйте на офлайн Windows**:
   - Каталог `s3Flood/` (исходники, конфиги).
   - Папку `wheels/`.
   - Установщики `python-3.11.x-amd64.exe`, `AWSCLIV2.msi`.

3. **На офлайн Windows**:
   ```powershell
   # установить Python и AWS CLI (installer'ы запускать от администратора)

   cd C:\path\to\s3Flood
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate

   pip install --no-index --find-links .\wheels s3flood==0.3.0
   ```
   После этого доступны `python -m s3flood ...` и `aws ...`.

4. **Если нужен датасет** — создайте его прямо на Windows без `--use-symlinks`:
   ```powershell
   python -m s3flood dataset-create --path .\loadset --target-bytes 5GB
   ```
   > Совет: держите `--group-limits` ≤5 ГБ, чтобы каждый файл загружался одним запросом `put-object`. Это защищает от `BadDigest` на несовместимых S3-бэкендах, где multipart CRC32 не поддерживается.

5. **Готовый ZIP для Windows**. Workflow `windows-bundle` автоматически собирает `s3flood.exe` + `config.sample.yaml` + README и публикует архив `s3flood-windows.zip` в релизах GitHub. На офлайн машине достаточно:
   - установить AWS CLI;
   - распаковать архив из релиза;
   - запускать `.\s3flood.exe run --config config.yaml`.

#### Локальная сборка Windows exe (для разработки)

Если нужно собрать `s3flood.exe` локально на Windows для тестирования:

```powershell
# 1. Установить зависимости
python -m pip install --upgrade pip
pip install -e .
pip install pyinstaller

# 2. Собрать exe
pyinstaller --name s3flood --onefile --paths src --collect-all s3flood --collect-all pydantic --collect-all rich --collect-all yaml s3flood_entry.py

# 3. Проверить
.\dist\s3flood.exe --help
.\dist\s3flood.exe dataset-create --help
.\dist\s3flood.exe run --help
```

Готовый `s3flood.exe` будет в папке `dist\`.

### CLI

После установки через `./install.sh` используйте wrapper скрипт:

Создание датасета (по свободному месту на диске):
```bash
./s3flood dataset-create --path ./loadset --use-symlinks
```

Запуск записи (пример, MinIO):
```bash
./s3flood run \
  --profile write-heavy \
  --client awscli \
  --threads 8 \
  --endpoint http://127.0.0.1:9000 \
  --bucket test-bucket \
  --access-key minioadmin \
  --secret-key minioadmin \
  --data-dir ./loadset/data \
  --report out.json \
  --metrics out.csv
```

Альтернативно, если venv активирован:
```bash
python -m s3flood dataset-create --path ./loadset --use-symlinks
```

Форматы вывода:
- CSV `metrics.csv|out.csv`: построчно на каждую операцию — время начала/окончания, тип операции, байты, статус, латентность, ошибка (если была).
- JSON `report.json|out.json`: итоговые агрегаты — длительность, средний throughput чтения/записи, количество успешных/ошибочных операций.

### Локальный сценарий (MinIO)
1. Запустить MinIO:
   ```bash
   docker run -d --name minio \
     -p 9000:9000 -p 9001:9001 \
     -e MINIO_ROOT_USER=minioadmin \
     -e MINIO_ROOT_PASSWORD=minioadmin \
     quay.io/minio/minio server /data --console-address ":9001"
   ```
2. Подготовить датасет (пример для каталога `./loadset`):
   ```bash
   ./s3flood dataset-create \
     --path ./loadset \
     --target-bytes auto \
     --use-symlinks \
     --min-counts 100,50,20 \
     --group-limits 100MB,1GB,10GB \
     --safety-ratio 0.8
   ```
   > По умолчанию `--target-bytes auto` забирает до 80 % свободного места. Для компактного стенда (≈5 ГБ) используйте, например:
   > ```bash
   > python -m s3flood dataset-create ^
   >   --path .\loadset ^
   >   --target-bytes 5GB ^
   >   --min-counts 20,10,5 ^
   >   --group-limits 50MB,500MB,2GB
   > ```
3. Запустить нагрузку (write-heavy профиль):
   ```bash
   ./s3flood run \
     --profile write-heavy \
     --client awscli \
     --endpoint http://localhost:9000 \
     --bucket test-bucket \
     --access-key minioadmin \
     --secret-key minioadmin \
     --threads 8 \
     --data-dir ./loadset/data \
     --report out.json \
     --metrics out.csv
   ```
4. Проверить результаты:
   - Убедиться, что `out.csv` содержит построчные операции без ошибок.
   - В `out.json` посмотреть агрегаты throughput/latency.
   - При необходимости повторно запустить `dataset-create` (например, с `--use-symlinks` для быстрого обновления).

### Конфигурация запуска и реальные бэкенды
- Создайте файл `config.local.yaml` (см. `config.sample.yaml`) и пропишите значения по умолчанию: `endpoint`, `bucket`, креденшлы, путь к датасету, имя профиля.
- Запуск с конфигом:
  ```bash
  ./s3flood run --config config.local.yaml
  ```
- Любой флаг CLI перекрывает значение из конфига (например, `--threads 16` или `--profile read-heavy` в будущем).
- Аутентификация:
  - Можно задать `access_key`/`secret_key` в конфиге или CLI.
  - Либо указать `aws_profile` (или `--aws-profile`) — будет использован выбранный профиль AWS CLI.
  - Если ни ключи, ни профиль не заданы, AWS CLI применит системную конфигурацию (`~/.aws/credentials`, переменные окружения).
- Кластерный режим:
  - Вместо `endpoint` можно указать `endpoints: ["http://node1:9000","http://node2:9000"]`.
  - Стратегия выбора задаётся `endpoint_mode: round-robin` (по умолчанию) или `random`.
  - Те же значения можно передать через CLI: `--endpoints http://node1:9000 http://node2:9000 --endpoint-mode random`.
- Для примера реального стенда (HTTP `192.168.20.35:9080`, bucket `cluster_test`) достаточно обновить значения в `config.local.yaml` и выполнить `python -m s3flood run --config config.local.yaml`.

### Профили нагрузки

- `write-heavy`: сначала 100% upload существующих файлов датасета (равномерно по группам размеров), затем автоматически начинается фаза чтения тех же файлов в `/dev/null` (без нагрузки на диск). Используется то же количество потоков для обеих фаз.

- `read-heavy`: сначала выполняется полная загрузка всех файлов датасета, затем начинается фаза интенсивного чтения тех же объектов. Полезен для тестирования пропускной способности чтения S3-бекенда.

- `mixed-70-30`: сначала выполняется загрузка всех файлов, затем начинается смешанная фаза с одновременными операциями чтения и записи. По умолчанию 70% операций — чтение, 30% — запись (настраивается через `mixed_read_ratio`). Поддерживает паттерны `sustained` и `bursty`.

### Паттерны нагрузки

- `sustained` (по умолчанию): ровная постоянная нагрузка без всплесков.
- `bursty`: чередование периодов высокой и низкой нагрузки. Параметры:
  - `burst_duration_sec`: длительность всплеска в секундах (по умолчанию 10.0)
  - `burst_intensity_multiplier`: множитель интенсивности во время всплеска (по умолчанию 10.0)

### Параметры конфигурации

В конфигурационном файле можно задать:

```yaml
run:
  profile: mixed-70-30
  mixed_read_ratio: 0.7  # Доля операций чтения (0.0-1.0)
  pattern: bursty  # sustained | bursty
  burst_duration_sec: 10.0
  burst_intensity_multiplier: 10.0
  queue_limit: 1000  # Максимальный размер очереди операций
  max_retries: 3  # Количество повторов при ошибке
  retry_backoff_base: 2.0  # Базовый множитель для экспоненциального backoff
```

### Кластерный режим

В кластерном режиме объекты автоматически привязываются к endpoint'у при записи и читаются через тот же endpoint. Это обеспечивает консистентность данных и позволяет тестировать распределённые S3-кластеры.

### Группы размеров по умолчанию
- small: до 100MB (доля байтов ~30%)
- medium: до 1GB (доля байтов ~50%)
- large: до 10GB (доля байтов ~20%)
- Минимальное количество объектов: small ≥100, medium ≥50, large ≥20.
- При `--target-bytes auto` используется 80% свободного места (`--safety-ratio 0.8`).

### Лицензия
MIT. См. LICENSE.

### Версии и релизы
- Версия пакета хранится в `pyproject.toml` и `VERSION`.
- Тег формата `vX.Y.Z` в GitHub автоматически запускает workflow `Release`, который собирает wheel/sdist и публикует GitHub Release с артефактами.
- Изменения фиксируем через `CHANGELOG.md`.
