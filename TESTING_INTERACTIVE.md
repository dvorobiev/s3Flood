# Тестирование интерактивного меню

## Установка зависимостей

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск интерактивного меню

### Вариант 1: Без аргументов (автоматически запускается меню)
```bash
python -m s3flood
```

### Вариант 2: С явным флагом
```bash
python -m s3flood --interactive
# или
python -m s3flood -i
```

### Вариант 3: Через wrapper скрипт
```bash
./s3flood
# или
./s3flood --interactive
```

## Проверка обратной совместимости

Все старые команды должны работать как раньше:

```bash
# Создание датасета
python -m s3flood dataset-create --path ./loadset --target-bytes 1GB

# Запуск теста
python -m s3flood run --profile write --endpoint http://localhost:9000 --bucket test
```

## Структура файлов

- `src/s3flood/interactive.py` - основной модуль с меню
- `src/s3flood/cli.py` - интеграция в CLI (добавлен флаг --interactive)

## Навигация в меню

- **Tab/Shift+Tab** - переключение между элементами
- **Enter** - выбор/активация
- **q** - выход из меню
- **Esc** - возврат назад (на экранах подменю)

## Текущий статус

✅ Этап 1: Базовая структура меню (заглушки)
⏳ Этап 2: Форма создания датасета
⏳ Этап 3: Форма редактирования конфига
⏳ Этап 4: Проверка конфига
⏳ Этап 5: Запуск теста через меню
⏳ Этап 6: Просмотр метрик

