#!/bin/bash
# Скрипт для синхронизации репозитория с корпоративным Gitea
# Использование: 
#   ./sync-to-corporate.sh [URL] [--force]
#   или
#   GITEA_URL=https://git.archive.systems/dvorobiev/s3Flood.git ./sync-to-corporate.sh [--force]
#
# Примеры:
#   ./sync-to-corporate.sh
#   ./sync-to-corporate.sh https://git.archive.systems/dvorobiev/s3Flood.git
#   ./sync-to-corporate.sh --force
#   GITEA_URL=https://gitea.example.com/user/repo.git ./sync-to-corporate.sh

set -e

CORPORATE_REMOTE="corporate"
# URL по умолчанию (можно переопределить через переменную окружения или аргумент)
DEFAULT_URL="https://git.archive.systems/dvorobiev/s3Flood.git"

# Определяем URL репозитория
CORPORATE_URL="${GITEA_URL:-$DEFAULT_URL}"

# Обработка аргументов
FORCE_PUSH=false
for arg in "$@"; do
    case $arg in
        --force)
            FORCE_PUSH=true
            echo "⚠️  Включён режим force push"
            ;;
        http://*|https://*|git@*)
            CORPORATE_URL="$arg"
            ;;
        *)
            echo "⚠️  Неизвестный аргумент: $arg"
            ;;
    esac
done

echo "🔄 Синхронизация с корпоративным Gitea..."
echo "📍 URL репозитория: $CORPORATE_URL"

# Проверяем, существует ли remote
if ! git remote get-url "$CORPORATE_REMOTE" >/dev/null 2>&1; then
    echo "➕ Добавляем remote для корпоративного репозитория..."
    git remote add "$CORPORATE_REMOTE" "$CORPORATE_URL"
else
    # Обновляем URL если он изменился
    CURRENT_URL=$(git remote get-url "$CORPORATE_REMOTE")
    if [ "$CURRENT_URL" != "$CORPORATE_URL" ]; then
        echo "🔄 Обновляем URL remote с $CURRENT_URL на $CORPORATE_URL"
        git remote set-url "$CORPORATE_REMOTE" "$CORPORATE_URL"
    fi
fi

# Получаем все ветки и теги из origin
echo "📥 Получаем последние изменения из GitHub..."
git fetch origin --prune --tags

# Функция для отправки ветки без CI файлов
push_branch_without_ci() {
    local branch=$1
    local source_ref=$2
    local target_ref="refs/heads/$branch"
    
    echo "  → Ветка: $branch (без CI/CD файлов)"
    
    # Создаём временную ветку для синхронизации без .github/
    TEMP_BRANCH="sync-corporate-$$"
    
    # Проверяем, существует ли уже эта ветка
    if git show-ref --verify --quiet "refs/heads/$TEMP_BRANCH"; then
        git branch -D "$TEMP_BRANCH" 2>/dev/null || true
    fi
    
    # Создаём временную ветку из исходной
    git checkout -b "$TEMP_BRANCH" "$source_ref" >/dev/null 2>&1
    
    # Удаляем .github/ из индекса (но не из рабочей директории)
    if git ls-files --error-unmatch .github/ >/dev/null 2>&1; then
        git rm -r --cached .github/ >/dev/null 2>&1 || true
        # Коммитим изменения (удаление .github/)
        if ! git diff --cached --quiet; then
            git commit -m "Remove CI/CD files for corporate sync" >/dev/null 2>&1 || true
        fi
    fi
    
    # Пушим временную ветку
    if [ "$FORCE_PUSH" = true ]; then
        git push "$CORPORATE_REMOTE" "$TEMP_BRANCH:$target_ref" --force || {
            echo "    ⚠️  Ошибка при отправке ветки $branch"
            git checkout - >/dev/null 2>&1
            git branch -D "$TEMP_BRANCH" 2>/dev/null || true
            return 1
        }
    else
        git push "$CORPORATE_REMOTE" "$TEMP_BRANCH:$target_ref" || {
            echo "    ⚠️  Ошибка при отправке ветки $branch"
            git checkout - >/dev/null 2>&1
            git branch -D "$TEMP_BRANCH" 2>/dev/null || true
            return 1
        }
    fi
    
    # Возвращаемся на исходную ветку и удаляем временную
    git checkout - >/dev/null 2>&1
    git branch -D "$TEMP_BRANCH" 2>/dev/null || true
}

# Получаем список всех веток из origin
echo "📤 Отправляем все ветки в корпоративный репозиторий (без CI/CD)..."
BRANCHES=$(git branch -r --format='%(refname:short)' | grep '^origin/' | sed 's|origin/||' | grep -v HEAD)

if [ -z "$BRANCHES" ]; then
    echo "  ℹ️  Нет веток для отправки"
else
    CURRENT_BRANCH_SAVED=$(git branch --show-current)
    for branch in $BRANCHES; do
        push_branch_without_ci "$branch" "origin/$branch"
    done
    # Возвращаемся на исходную ветку если нужно
    if [ -n "$CURRENT_BRANCH_SAVED" ]; then
        git checkout "$CURRENT_BRANCH_SAVED" >/dev/null 2>&1 || true
    fi
fi

# Пушим текущую ветку, если она локальная и не синхронизирована
CURRENT_BRANCH=$(git branch --show-current)
if [ -n "$CURRENT_BRANCH" ] && ! git branch -r | grep -q "origin/$CURRENT_BRANCH"; then
    echo "  → Локальная ветка: $CURRENT_BRANCH (без CI/CD файлов)"
    push_branch_without_ci "$CURRENT_BRANCH" "$CURRENT_BRANCH"
fi

# Пушим все теги
echo "🏷️  Отправляем все теги..."
if [ "$FORCE_PUSH" = true ]; then
    git push "$CORPORATE_REMOTE" --tags --force || echo "  ⚠️  Ошибка при отправке тегов"
else
    git push "$CORPORATE_REMOTE" --tags || echo "  ⚠️  Ошибка при отправке тегов"
fi

echo ""
echo "✅ Синхронизация завершена!"
echo "📋 Корпоративный репозиторий: $CORPORATE_URL"
echo ""
echo "💡 Если возникли ошибки доступа:"
echo "   1. Убедитесь, что VPN подключен"
echo "   2. Проверьте доступ к Gitea: curl -I $CORPORATE_URL"
echo "   3. Для HTTPS: используйте Personal Access Token в URL"
echo "      https://username:token@gitea.example.com/user/repo.git"
echo "   4. Для SSH: настройте SSH ключи в Gitea и используйте git@..."

