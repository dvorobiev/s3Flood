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

# Функция для гарантированной очистки при выходе
cleanup() {
    local exit_code=$?
    # Удаляем неотслеживаемые файлы .github/
    rm -rf .github/ 2>/dev/null || true
    
    # Возвращаемся на исходную ветку
    if [ -n "$ORIGINAL_BRANCH_SAVED" ]; then
        git checkout "$ORIGINAL_BRANCH_SAVED" >/dev/null 2>&1 || git checkout main >/dev/null 2>&1 || true
    else
        git checkout main >/dev/null 2>&1 || true
    fi
    
    # Удаляем временную ветку если она существует
    if [ -n "$TEMP_BRANCH" ]; then
        git branch -D "$TEMP_BRANCH" 2>/dev/null || true
    fi
    
    # Удаляем временные файлы
    rm -f /tmp/git_push_output_*.txt 2>/dev/null || true
    
    exit $exit_code
}

# Регистрируем trap для очистки при любом выходе
trap cleanup EXIT INT TERM

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

# Очистка перед началом работы - проверяем, не находимся ли мы на временной ветке
CURRENT_BRANCH_BEFORE=$(git branch --show-current 2>/dev/null || echo "")
if [[ "$CURRENT_BRANCH_BEFORE" == sync-corporate-* ]]; then
    echo "⚠️  Обнаружена временная ветка $CURRENT_BRANCH_BEFORE, очищаем и переключаемся на main..."
    # Удаляем неотслеживаемые файлы .github/ которые могут мешать
    rm -rf .github/ 2>/dev/null || true
    git checkout main >/dev/null 2>&1 || true
    git branch -D "$CURRENT_BRANCH_BEFORE" 2>/dev/null || true
fi

# Очищаем все оставшиеся временные ветки
git branch | grep "^  sync-corporate-" | sed 's/^  //' | xargs -r git branch -D 2>/dev/null || true

# Получаем все ветки и теги из origin
echo "📥 Получаем последние изменения из GitHub..."
git fetch origin --prune --tags

# Функция для отправки ветки без CI файлов
push_branch_without_ci() {
    local branch=$1
    local source_ref=$2
    local target_ref="refs/heads/$branch"
    
    # Сохраняем исходную ветку ДО всех операций
    ORIGINAL_BRANCH_SAVED=$(git branch --show-current 2>/dev/null || echo "main")
    
    echo "  → Ветка: $branch (без CI/CD файлов)"
    
    # Создаём временную ветку для синхронизации без .github/
    TEMP_BRANCH="sync-corporate-$$"
    
    # Проверяем, существует ли уже эта ветка
    if git show-ref --verify --quiet "refs/heads/$TEMP_BRANCH"; then
        git branch -D "$TEMP_BRANCH" 2>/dev/null || true
    fi
    
    # Убеждаемся, что мы на правильной ветке перед созданием временной
    git checkout "$ORIGINAL_BRANCH_SAVED" >/dev/null 2>&1 || git checkout main >/dev/null 2>&1 || true
    
    # Удаляем временную ветку если она уже существует
    if git show-ref --verify --quiet "refs/heads/$TEMP_BRANCH"; then
        git branch -D "$TEMP_BRANCH" >/dev/null 2>&1 || true
    fi
    
    # Создаём временную ветку из исходной
    if ! git checkout -b "$TEMP_BRANCH" "$source_ref" 2>&1; then
        echo "    ⚠️  Ошибка при создании временной ветки из $source_ref"
        echo "    💡 Убедитесь, что ветка $source_ref существует"
        git checkout "$ORIGINAL_BRANCH_SAVED" >/dev/null 2>&1 || git checkout main >/dev/null 2>&1 || true
        return 1
    fi
    
    # Удаляем .github/ из индекса (но не из рабочей директории)
    if git ls-files --error-unmatch .github/ >/dev/null 2>&1; then
        git rm -r --cached .github/ >/dev/null 2>&1 || true
        # Коммитим изменения (удаление .github/)
        if ! git diff --cached --quiet; then
            git commit -m "Remove CI/CD files for corporate sync" >/dev/null 2>&1 || true
        fi
    fi
    
    # Пушим временную ветку с таймаутом
    PUSH_SUCCESS=false
    PUSH_OUTPUT=""
    PUSH_TIMEOUT=30  # Таймаут 30 секунд
    PUSH_OUTPUT_FILE="/tmp/git_push_output_$$.txt"
    
    echo "    ⏳ Отправка ветки $branch (таймаут ${PUSH_TIMEOUT}с)..."
    
    # Функция для push с таймаутом (работает на macOS и Linux)
    push_with_timeout() {
        local push_cmd="$1"
        local output_file="$2"
        local timeout_sec="$3"
        
        # Запускаем push в фоне
        eval "$push_cmd" > "$output_file" 2>&1 &
        local push_pid=$!
        
        # Ждём завершения или таймаута
        local elapsed=0
        while [ $elapsed -lt $timeout_sec ]; do
            if ! kill -0 $push_pid 2>/dev/null; then
                # Процесс завершился
                wait $push_pid
                return $?
            fi
            sleep 1
            elapsed=$((elapsed + 1))
        done
        
        # Таймаут - убиваем процесс
        if kill -0 $push_pid 2>/dev/null; then
            kill $push_pid 2>/dev/null || true
            wait $push_pid 2>/dev/null || true
            echo "Timeout: push занял больше ${timeout_sec} секунд" >> "$output_file"
            return 1
        fi
        
        wait $push_pid
        return $?
    }
    
    if [ "$FORCE_PUSH" = true ]; then
        push_cmd="git push \"$CORPORATE_REMOTE\" \"$TEMP_BRANCH:$target_ref\" --force"
        if push_with_timeout "$push_cmd" "$PUSH_OUTPUT_FILE" "$PUSH_TIMEOUT"; then
            PUSH_EXIT=0
        else
            PUSH_EXIT=1
        fi
        PUSH_OUTPUT=$(cat "$PUSH_OUTPUT_FILE" 2>/dev/null || echo "")
        rm -f "$PUSH_OUTPUT_FILE"
        
        if [ $PUSH_EXIT -eq 0 ]; then
            PUSH_SUCCESS=true
            echo "    ✅ Ветка $branch успешно отправлена (force)"
        else
            echo "    ⚠️  Ошибка при force push ветки $branch"
            echo "$PUSH_OUTPUT" | grep -E "(error|fatal|rejected|Timeout)" | head -3 | sed 's/^/    /'
        fi
    else
        push_cmd="git push \"$CORPORATE_REMOTE\" \"$TEMP_BRANCH:$target_ref\""
        if push_with_timeout "$push_cmd" "$PUSH_OUTPUT_FILE" "$PUSH_TIMEOUT"; then
            PUSH_EXIT=0
        else
            PUSH_EXIT=1
        fi
        PUSH_OUTPUT=$(cat "$PUSH_OUTPUT_FILE" 2>/dev/null || echo "")
        rm -f "$PUSH_OUTPUT_FILE"
        
        if [ $PUSH_EXIT -eq 0 ]; then
            PUSH_SUCCESS=true
            echo "    ✅ Ветка $branch успешно отправлена"
        else
            # Проверяем, не требуется ли force push
            if echo "$PUSH_OUTPUT" | grep -q "non-fast-forward\|rejected"; then
                echo "    ⚠️  Ветка $branch отклонена (non-fast-forward)"
                echo "    💡 В Gitea уже есть коммиты, которых нет локально."
                echo "    💡 Используйте --force для принудительной отправки (осторожно!)"
            elif echo "$PUSH_OUTPUT" | grep -q "Timeout"; then
                echo "    ⚠️  Таймаут при отправке ветки $branch"
                echo "    💡 Проверьте доступ к корпоративному Gitea (VPN, сеть)"
            else
                echo "    ⚠️  Ошибка при отправке ветки $branch"
                echo "$PUSH_OUTPUT" | grep -E "(error|fatal)" | head -3 | sed 's/^/    /'
            fi
        fi
    fi
    
    # Очистка выполняется через trap cleanup
    # Но можно явно удалить неотслеживаемые файлы здесь
    rm -rf .github/ 2>/dev/null || true
    
    if [ "$PUSH_SUCCESS" = false ]; then
        return 1
    fi
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
    # Очищаем все оставшиеся временные ветки (на случай если что-то пошло не так)
    git branch | grep "^  sync-corporate-" | xargs -r git branch -D 2>/dev/null || true
fi

# Финальная проверка и очистка - гарантируем, что мы на правильной ветке
FINAL_CLEANUP_BRANCH=$(git branch --show-current 2>/dev/null || echo "")
if [[ "$FINAL_CLEANUP_BRANCH" == sync-corporate-* ]]; then
    echo "⚠️  Обнаружена временная ветка, возвращаемся на main..."
    git checkout main >/dev/null 2>&1 || true
    git branch -D "$FINAL_CLEANUP_BRANCH" 2>/dev/null || true
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

