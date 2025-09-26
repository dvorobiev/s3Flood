# Инструкция по установке S3 Flood на Windows

## Автоматическая установка

1. Скачайте проект с GitHub:
   - Перейдите на https://github.com/dvorobiev/s3Flood
   - Нажмите "Code" → "Download ZIP"
   - Распакуйте архив в удобную папку

2. Убедитесь, что установлен Python:
   - Скачайте с https://www.python.org/downloads/
   - При установке обязательно отметьте "Add Python to PATH"

3. Запустите установку:
   - Откройте папку с проектом
   - Дважды щелкните на `install.bat`
   - Дождитесь завершения установки

## Запуск

- Дважды щелкните на `run.bat`
- Или откройте командную строку и выполните: `python s3_flood.py`

## Если возникли проблемы

1. Проверьте, что Python установлен:
   ```cmd
   python --version
   ```

2. Установите зависимости вручную:
   ```cmd
   pip install -r requirements.txt
   ```

3. Для работы с S3 нужен s5cmd:
   - Скачайте с https://github.com/peak/s5cmd/releases
   - Положите s5cmd.exe в папку проекта или добавьте в PATH

