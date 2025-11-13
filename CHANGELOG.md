## Changelog

### 0.2.0 — конфиги и кластерные endpoint'ы
- Поддержка `config.py`: запуск через YAML, AWS CLI профиль, объединение с CLI-флагами.
- Пример конфига `config.sample.yaml`, описание в README.
- `run` принимает `--config`, `--endpoints`, `--endpoint-mode`, `--aws-profile`.
- Кластерный режим: выбор endpoint'а по round-robin или random.
- В итоговом отчёте `SUMMARY` появились поля `write_bytes` и `read_bytes`.

### 0.1.0 — initial cleaned CLI prototype
- Новая минимальная структура `src/s3flood/` (CLI, dataset, executor).
- Генерация датасета с симлинками и сид-файлами.
- Профиль записи с AWS CLI (многопоточность).
- Метрики CSV и итоговый отчёт JSON.
- Очистка репозитория от устаревших скриптов и артефактов.


