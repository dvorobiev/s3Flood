#!/bin/bash
# Скрипт установки s3flood для Mac и Linux
# Использование: ./install.sh [--python-version 3.12]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
MIN_PYTHON_VERSION="3.10"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}✓${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

echo_error() {
    echo -e "${RED}✗${NC} $1"
}

# Проверка наличия Python
check_python() {
    echo "🔍 Проверка Python..."
    
    # Пробуем найти Python нужной версии
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    elif command -v python &> /dev/null; then
        PYTHON_CMD="python"
    else
        echo_error "Python не найден. Установите Python $MIN_PYTHON_VERSION или выше."
        echo ""
        echo "Для Mac:"
        echo "  brew install python@$PYTHON_VERSION"
        echo ""
        echo "Для Debian/Ubuntu:"
        echo "  sudo apt-get update"
        echo "  sudo apt-get install python3 python3-venv python3-pip"
        echo ""
        echo "Для Debian старых версий (если python3.10+ недоступен):"
        echo "  sudo apt-get install software-properties-common"
        echo "  sudo add-apt-repository ppa:deadsnakes/ppa"
        echo "  sudo apt-get update"
        echo "  sudo apt-get install python3.10 python3.10-venv python3.10-dev"
        exit 1
    fi
    
    # Проверяем версию
    PYTHON_VER=$($PYTHON_CMD --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
    PYTHON_MAJOR=$(echo $PYTHON_VER | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VER | cut -d. -f2)
    MIN_MAJOR=$(echo $MIN_PYTHON_VERSION | cut -d. -f1)
    MIN_MINOR=$(echo $MIN_PYTHON_VERSION | cut -d. -f2)
    
    if [ "$PYTHON_MAJOR" -lt "$MIN_MAJOR" ] || \
       ([ "$PYTHON_MAJOR" -eq "$MIN_MAJOR" ] && [ "$PYTHON_MINOR" -lt "$MIN_MINOR" ]); then
        echo_error "Требуется Python $MIN_PYTHON_VERSION или выше, найдено: $PYTHON_VER"
        exit 1
    fi
    
    echo_info "Python $PYTHON_VER найден"
}

# Проверка AWS CLI
check_aws_cli() {
    echo "🔍 Проверка AWS CLI..."
    
    if ! command -v aws &> /dev/null; then
        echo_warn "AWS CLI не найден в PATH"
        echo ""
        echo "Установите AWS CLI:"
        echo ""
        echo "Для Mac:"
        echo "  brew install awscli"
        echo ""
        echo "Для Debian/Ubuntu:"
        echo "  curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'"
        echo "  unzip awscliv2.zip"
        echo "  sudo ./aws/install"
        echo ""
        echo "Или через pip:"
        echo "  pip install awscli"
        echo ""
        read -p "Продолжить установку без AWS CLI? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        AWS_VERSION=$(aws --version 2>&1 | head -n1)
        echo_info "AWS CLI найден: $AWS_VERSION"
    fi
}

# Создание venv
create_venv() {
    echo "📦 Создание виртуального окружения..."
    
    if [ -d "$VENV_DIR" ]; then
        echo_warn "Виртуальное окружение уже существует в $VENV_DIR"
        read -p "Пересоздать? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$VENV_DIR"
        else
            echo_info "Используем существующее окружение"
            return
        fi
    fi
    
    $PYTHON_CMD -m venv "$VENV_DIR"
    echo_info "Виртуальное окружение создано в $VENV_DIR"
}

# Установка зависимостей
install_dependencies() {
    echo "📥 Установка зависимостей..."
    
    # Активируем venv
    source "$VENV_DIR/bin/activate"
    
    # Обновляем pip
    echo "  → Обновление pip..."
    pip install --upgrade pip setuptools wheel > /dev/null 2>&1
    
    # Устанавливаем проект
    echo "  → Установка s3flood..."
    pip install -e "$SCRIPT_DIR" > /dev/null 2>&1
    
    echo_info "Зависимости установлены"
}

# Создание wrapper скрипта
create_wrapper() {
    echo "🔧 Создание wrapper скрипта..."
    
    WRAPPER_SCRIPT="$SCRIPT_DIR/s3flood"
    
    cat > "$WRAPPER_SCRIPT" << 'EOF'
#!/bin/bash
# Wrapper скрипт для запуска s3flood с автоматической активацией venv

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

# Проверяем наличие venv
if [ ! -d "$VENV_DIR" ]; then
    echo "Ошибка: виртуальное окружение не найдено в $VENV_DIR"
    echo "Запустите ./install.sh для установки"
    exit 1
fi

# Активируем venv и запускаем команду
source "$VENV_DIR/bin/activate"
exec python -m s3flood "$@"
EOF
    
    chmod +x "$WRAPPER_SCRIPT"
    echo_info "Wrapper скрипт создан: $WRAPPER_SCRIPT"
}

# Основная установка
main() {
    echo "🚀 Установка s3flood"
    echo "==================="
    echo ""
    
    cd "$SCRIPT_DIR"
    
    check_python
    check_aws_cli
    create_venv
    install_dependencies
    create_wrapper
    
    echo ""
    echo "✅ Установка завершена!"
    echo ""
    echo "Использование:"
    echo "  ./s3flood dataset-create --path ./loadset --use-symlinks"
    echo "  ./s3flood run --profile write-heavy --endpoint http://localhost:9000 --bucket test-bucket"
    echo ""
    echo "Или через Python модуль:"
    echo "  source .venv/bin/activate"
    echo "  python -m s3flood ..."
    echo ""
}

main "$@"

