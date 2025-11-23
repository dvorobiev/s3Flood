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

Все параметры можно указать в YAML-конфиге или через CLI-флаги. Параметры из конфига можно переопределить через CLI.

#### Основные параметры

- **`profile`** (обязательно): Профиль нагрузки
  - `write` — только запись файлов из датасета в бакет
  - `read` — только чтение объектов из бакета (в `/dev/null`)
  - `mixed-70-30` — смешанные операции (70% чтение, 30% запись)

- **`client`** (по умолчанию: `awscli`): S3 клиент для использования
  - `awscli`, `rclone`, `s3cmd`

- **`endpoint`** (обязательно, если не указан `endpoints`): URL S3 endpoint
  - Пример: `http://localhost:9000` для MinIO

- **`endpoints`** (опционально): Список endpoint'ов для кластерного режима
  - Пример: `["http://node1:9000", "http://node2:9000"]`

- **`endpoint_mode`** (по умолчанию: `round-robin`): Стратегия выбора endpoint'а
  - `round-robin` — по кругу
  - `random` — случайно

- **`bucket`** (обязательно): Имя S3 бакета для тестирования

#### Аутентификация

- **`access_key`**: AWS Access Key ID (или S3-совместимый ключ доступа)
- **`secret_key`**: AWS Secret Access Key (или S3-совместимый секретный ключ)
- **`aws_profile`**: Имя профиля AWS CLI (из `~/.aws/credentials`)
  - Альтернатива `access_key`/`secret_key`

#### Параметры выполнения

- **`threads`** (по умолчанию: `8`): Количество параллельных потоков для операций
- **`data_dir`** (по умолчанию: `./data`): Путь к корню датасета (сканируется рекурсивно)
- **`report`** (по умолчанию: `report.json`): Путь к JSON файлу с итоговым отчётом
- **`metrics`** (по умолчанию: `metrics.csv`): Путь к CSV файлу с детальными метриками по каждой операции
- **`infinite`** (по умолчанию: `false`): Бесконечный режим — после завершения всех файлов начинать заново

#### Профиль mixed-70-30

- **`mixed_read_ratio`** (по умолчанию: `0.7` для `mixed-70-30`): Доля операций чтения (0.0-1.0)
  - `0.7` означает 70% чтение, 30% запись

#### Паттерны нагрузки

- **`pattern`** (по умолчанию: `sustained`): Паттерн нагрузки
  - `sustained` — ровная постоянная нагрузка
  - `bursty` — чередование периодов высокой и низкой нагрузки

- **`burst_duration_sec`** (по умолчанию: `10.0`): Длительность всплеска в секундах для `bursty` паттерна

- **`burst_intensity_multiplier`** (по умолчанию: `10.0`): Множитель интенсивности во время всплеска для `bursty` паттерна
  - Во время всплеска интенсивность увеличивается в указанное количество раз

#### Управление очередью и повторами

- **`queue_limit`** (по умолчанию: без ограничений): Максимальный размер очереди операций
  - При достижении лимита новые задачи не добавляются до освобождения места

- **`max_retries`** (по умолчанию: `3`): Максимальное количество повторов при ошибке

- **`retry_backoff_base`** (по умолчанию: `2.0`): Базовый множитель для экспоненциального backoff при повторах
  - При значении `2.0` задержки между попытками: 1s, 2s, 4s (2^0, 2^1, 2^2)
  - При значении `3.0` задержки: 1s, 3s, 9s (3^0, 3^1, 3^2)

#### Порядок обработки файлов

- **`order`** (по умолчанию: `sequential`): Порядок обработки файлов
  - `sequential` — сначала маленькие файлы, потом средние, потом большие
  - `random` — случайный порядок обработки файлов

#### Пример полного конфига

```yaml
run:
  profile: write
  endpoint: "http://localhost:9000"
  bucket: test
  access_key: "YOUR_ACCESS_KEY"
  secret_key: "YOUR_SECRET_KEY"
  threads: 8
  data_dir: "./loadset/data"
  report: "report.json"
  metrics: "metrics.csv"
  infinite: false
  order: sequential
  pattern: sustained
  max_retries: 3
  retry_backoff_base: 2.0
```

### Кластерный режим

Вместо `endpoint` можно указать `endpoints: ["http://node1:9000","http://node2:9000"]` с выбором стратегии `endpoint_mode: round-robin` или `random`. Объекты автоматически привязываются к endpoint'у при записи и читаются через тот же endpoint.

### Лицензия
MIT. См. LICENSE.
