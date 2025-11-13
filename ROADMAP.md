## Roadmap

Этап 1 — Минимальный CLI (сделано/в процессе)
- [x] Структура проекта (`src/`, `pyproject.toml`, ruff/black)
- [x] CLI: `dataset-create`, `run`
- [x] Dataset: группы small/medium/large, auto-планирование, симлинки
- [x] Executor: профиль `write-heavy` с AWS CLI, метрики CSV/JSON
- [ ] Базовые pytest-тесты и smoke в CI (MinIO)

Этап 2 — Клиенты и профили
- [ ] Интерфейс `Runner` и плагины: `s5cmd`, `rclone`, `awscli`
- [ ] Профили: `read-heavy`, `mixed-70-30`, `single-thread`, `multi-thread`
- [ ] Параметры: `--pattern sustained|bursty`, `--warmup`
- [ ] Ретраи с экспоненциальным backoff, таймауты, лимиты очередей

Этап 3 — Метрики и отчёты
- [ ] P50/P90/P99 latency для всех операций
- [ ] Экспорт: CSV (операции), JSON (аггрегаты), опц. Prometheus-текст
- [ ] Удобный TUI (необязательно)

Этап 4 — Удобство и дистрибуция
- [ ] Примеры конфигов (`examples/configs/*.yaml`)
- [ ] Документация по профилям и клиентам, Quickstart Linux/Windows
- [ ] PyInstaller сборки (win/linux) — опционально

Этап 5 — Оптимизации
- [ ] Пул задач и ограничения параллельности (IO/CPU)
- [ ] Ограничения IOPS/throughput, распределения размеров (Pareto/Zipf)


