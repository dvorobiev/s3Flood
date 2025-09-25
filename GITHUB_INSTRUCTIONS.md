# Инструкции по загрузке проекта на GitHub

## Шаг 1: Создание репозитория на GitHub

1. Перейдите на https://github.com/new
2. Введите название репозитория, например: `s3-flood`
3. Выберите видимость репозитория (Public или Private)
4. **Не** инициализируйте репозиторий с README, .gitignore или license
5. Нажмите "Create repository"

## Шаг 2: Подключение локального репозитория к GitHub

После создания репозитория на GitHub, выполните следующие команды в терминале:

```bash
cd /Users/dvorobiev/s3Flood
# Если у вас есть Personal Access Token (PAT), используйте его вместо пароля
git remote add origin https://github.com/ваш_логин/s3-flood.git
git branch -M main
git push -u origin main
```

Замените `ваш_логин` на ваше имя пользователя на GitHub.

Если GitHub запрашивает имя пользователя и пароль, введите:
- Имя пользователя: ваш логин на GitHub
- Пароль: ваш Personal Access Token (не обычный пароль)

## Шаг 3: Создание Personal Access Token (если еще не создан)

GitHub больше не поддерживает аутентификацию по паролю для Git операций. Вместо этого нужно использовать Personal Access Token:

1. Перейдите на https://github.com/settings/tokens
2. Нажмите "Generate new token"
3. Выберите нужные права (как минимум `repo`)
4. Сгенерируйте токен и сохраните его в безопасном месте

## Альтернатива: Использование SSH ключей

1. Сгенерируйте SSH ключ:
   ```bash
   ssh-keygen -t ed25519 -C "ваш_email@example.com"
   ```

2. Добавьте ключ в ssh-agent:
   ```bash
   eval "$(ssh-agent -s)"
   ssh-add ~/.ssh/id_ed25519
   ```

3. Добавьте публичный ключ в GitHub:
   - Скопируйте содержимое `~/.ssh/id_ed25519.pub`
   - Перейдите на https://github.com/settings/keys
   - Нажмите "New SSH key" и вставьте ключ

4. Используйте SSH URL для подключения:
   ```bash
   cd /Users/dvorobiev/s3Flood
   git remote set-url origin git@github.com:ваш_логин/s3-flood.git
   git push -u origin main
   ```

## Проверка загрузки

После выполнения команд проверьте, что код появился в вашем репозитории на GitHub.