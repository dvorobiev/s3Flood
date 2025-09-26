# S3 Flood Windows Installation Guide# S3 Flood - Windows Installation Guide

## 🚀 Быстрый старт (3 шага)

### Шаг 1: Установка
```batch
install.bat
```

### Шаг 2: Запуск
```batch
run_windows.bat
```

### Шаг 3: Готово! 🎉

---

## 📁 Что у нас есть (простая схема)

### ⭐ **Главные файлы** (используй их)
- **`install.bat`** - установщик
- **`run_windows.bat`** - запуск программы 
- **`s3_flood_ultra_safe.py`** - основная программа

### 🔄 **Запасные файлы** (если основные не работают)
- **`run_simple.bat`** - простой запуск
- **`s3_flood_simple.py`** - простая версия программы

### 📚 **Остальные файлы** 
- Разные версии и fallback'и (можно игнорировать)

---

## 🛠️ Подробная установка

### Автоматическая установка (рекомендуется)
1. Скачай Windows-support ветку с GitHub
2. Запусти `install.bat`
3. Установщик сам:
   - Проверит Python (если нет - скажет где скачать)
   - Установит PyYAML
   - Создаст папку tools/ для s5cmd
   - Создаст config.yaml

### Ручная установка
1. Установи Python 3.7+ с [python.org](https://www.python.org/downloads/)
2. При установке **ОБЯЗАТЕЛЬНО** поставь галку "Add Python to PATH"
3. Открой командную строку и выполни:
   ```batch
   pip install pyyaml
   ```
4. Запускай программу: `python s3_flood_ultra_safe.py`

---

## 🎮 Как запускать

### Способ 1: Основной (рекомендуется)
```batch
run_windows.bat
```
**Что происходит**: Запускается максимально совместимая версия

### Способ 2: Простой (если основной не работает)
```batch
run_simple.bat  
```
**Что происходит**: Запускается упрощенная версия

### Способ 3: Прямой (для отладки)
```batch
python s3_flood_ultra_safe.py
```
**Что происходит**: Запускается программа напрямую

---

## 🐛 Решение проблем

### "Python не найден"
**Решение**: 
1. Установи Python с [python.org](https://www.python.org/downloads/)
2. **ВАЖНО**: При установке поставь галку "Add Python to PATH"
3. Перезапусти командную строку
4. Проверь: `python --version`

### "The system cannot write to the specified device"
**Решение**: Используй `run_windows.bat` - эта версия решает проблемы с консолью

### "s5cmd не найден" или "Exception 0xc0000005"
**Решение**: Программа сама скачает правильную версию s5cmd для твоей Windows

### Кракозябры в консоли
**Решение**: Все batch файлы уже содержат `chcp 65001` для исправления кодировки

### Ошибки rich/questionary библиотек
**Решение**: Используй `run_windows.bat` - там нет этих библиотек

---

## ⚙️ Настройка

Отредактируй `config.yaml` или используй встроенное меню конфигурации:

```yaml
s3_urls: ["http://localhost:9000"]
access_key: "minioadmin"
secret_key: "minioadmin"
bucket_name: "test-bucket"
parallel_threads: 5
```

---

## 🎯 Что делать если ничего не работает

1. **Попробуй по порядку**:
   ```batch
   run_windows.bat
   run_simple.bat  
   python s3_flood_ultra_safe.py
   ```

2. **Проверь Python**:
   ```batch
   python --version
   ```

3. **Установи зависимости вручную**:
   ```batch
   pip install pyyaml
   ```

4. **Посмотри подробный гайд**: `WINDOWS_FILES_GUIDE.md`

---

## 🚀 Итого

**Для 99% пользователей нужно только**:
1. `install.bat` - один раз
2. `run_windows.bat` - каждый раз когда хочешь запустить

**Остальное** - это fallback'и на случай проблем! 🎉