## s3flood — минималистичный клиент для нагрузочного тестирования S3

Лёгкий консольный инструмент для генерации датасета и прогона профилей нагрузки к S3-совместимому бэкенду. Показывает прозрачный прогресс, пишет метрики в CSV и итоговый отчёт в JSON.

Сейчас реализован рабочий прототип:
- генерация датасета на локальном диске с использованием сидов и симлинков;
- профиль записи (upload) через AWS CLI;
- многопоточность выполнения;
- лаконичный прогресс и метрики (CSV/JSON).

Планируемая архитектура и расширения описаны в ROADMAP.md.

### Установка
- Требуется Python 3.10+ и установленный `aws` CLI в `PATH`.
- Локально:
```bash
pip install -e .
```

### CLI
Создание датасета (по свободному месту на диске):
```bash
python -m s3flood dataset-create --path ./loadset --use-symlinks
```

Запуск записи (пример, MinIO):
```bash
python -m s3flood run \
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
   python -m s3flood dataset-create \
     --path ./loadset \
     --target-bytes auto \
     --use-symlinks \
     --min-counts 100,50,20 \
     --group-limits 100MB,1GB,10GB \
     --safety-ratio 0.8
   ```
3. Запустить нагрузку (write-heavy профиль):
   ```bash
   python -m s3flood run \
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

### Профили (на сегодня)
- `write-heavy`: 100% upload существующих файлов датасета (равномерно по группам размеров).

### Группы размеров по умолчанию
- small: до 100MB (доля байтов ~30%)
- medium: до 1GB (доля байтов ~50%)
- large: до 10GB (доля байтов ~20%)
- Минимальное количество объектов: small ≥100, medium ≥50, large ≥20.
- При `--target-bytes auto` используется 80% свободного места (`--safety-ratio 0.8`).

### Лицензия
MIT. См. LICENSE.
