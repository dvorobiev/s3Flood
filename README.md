## s3flood — минималистичный клиент для нагрузочного тестирования S3

Лёгкий консольный инструмент для генерации датасета и прогона профилей нагрузки к S3-совместимому бэкенду. Показывает прозрачный прогресс, пишет метрики в CSV и итоговый отчёт в JSON.

### Установка

#### Mac и Linux

```bash
git clone https://github.com/dvorobiev/s3Flood.git
cd s3Flood
./install.sh
```

После установки: `./s3flood dataset-create --path ./loadset --target-bytes 1GB`

#### Windows без доступа к интернету

1. Скачайте `s3flood-windows-portable.zip` из [последнего релиза](https://github.com/dvorobiev/s3Flood/releases)
2. Распакуйте ZIP на Windows
3. Установите AWS CLI (если ещё не установлен)
4. Запускайте:
   ```powershell
   .\s3flood.bat dataset-create --path .\loadset --target-bytes 1GB
   .\s3flood.bat run --profile write --endpoint http://localhost:9000 --bucket test
   ```

> Совет: используйте `--group-limits` ≤5GB для защиты от ошибок `BadDigest` на некоторых S3-бэкендах.

### Использование

**Создать датасет:**
```bash
./s3flood dataset-create --path ./loadset --target-bytes 1GB
```

**Запустить тест:**
```bash
./s3flood run --profile write --endpoint http://localhost:9000 --bucket test --access-key minioadmin --secret-key minioadmin --data-dir ./loadset/data
```

Результаты: `metrics.csv` (детальные метрики) и `report.json` (итоговый отчёт).

### Быстрый старт (MinIO)

```bash
# 1. Запустить MinIO
docker run -d --name minio -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data --console-address ":9001"

# 2. Создать датасет
./s3flood dataset-create --path ./loadset --target-bytes 1GB

# 3. Запустить тест
./s3flood run --profile write --endpoint http://localhost:9000 --bucket test --access-key minioadmin --secret-key minioadmin --data-dir ./loadset/data
```

**Использование конфига:**
Создайте `config.yaml` на основе `config.sample.yaml` и запускайте: `./s3flood run --config config.yaml`

### Профили нагрузки

- `write`: только запись файлов из датасета в бакет
- `read`: только чтение объектов из бакета (в `/dev/null`, без нагрузки на диск)
- `mixed-70-30`: смешанные операции (70% чтение, 30% запись, настраивается через `mixed_read_ratio`)

**Порядок обработки файлов:**
- `--order sequential` (по умолчанию): сначала маленькие файлы, потом средние, потом большие
- `--order random`: случайный порядок обработки файлов

### Паттерны нагрузки

- `sustained` (по умолчанию): ровная постоянная нагрузка
- `bursty`: чередование периодов высокой и низкой нагрузки

### Параметры конфигурации

```yaml
run:
  profile: mixed-70-30
  mixed_read_ratio: 0.7
  pattern: bursty  # sustained | bursty
  burst_duration_sec: 10.0
  burst_intensity_multiplier: 10.0
  queue_limit: 1000
  max_retries: 3
  retry_backoff_base: 2.0
```

### Кластерный режим

Вместо `endpoint` можно указать `endpoints: ["http://node1:9000","http://node2:9000"]` с выбором стратегии `endpoint_mode: round-robin` или `random`. Объекты автоматически привязываются к endpoint'у при записи и читаются через тот же endpoint.

### Лицензия
MIT. См. LICENSE.
