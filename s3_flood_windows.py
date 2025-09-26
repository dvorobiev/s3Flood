#!/usr/bin/env python3
"""
S3 Flood Windows Version - Полностью совместимая версия для Windows
Эта версия работает без rich/questionary библиотек и решает проблемы с Windows console
"""

import os
import sys
import yaml
import subprocess
import platform
import zipfile
import urllib.request
import urllib.error
from pathlib import Path
import time
import threading
import random
import shutil
import signal
from datetime import datetime
from typing import List, Dict, Any, Optional

def get_version():
    """Get version from VERSION file"""
    try:
        with open(Path(__file__).parent / "VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "development"

class WindowsS3FloodTester:
    def __init__(self):
        self.config = {}
        self.s5cmd_path = self.ensure_s5cmd()
        self.stats = {
            "cycles_completed": 0,
            "files_uploaded": 0,
            "files_downloaded": 0,
            "files_deleted": 0,
            "total_upload_time": 0.0,
            "total_download_time": 0.0,
            "total_delete_time": 0.0,
            "total_bytes_uploaded": 0,
            "total_bytes_downloaded": 0,
            "upload_times": []
        }
        self.running = True
        self.local_temp_dir = Path("./s3_temp_files")
        
        # Register signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print("[INFO] Остановка S3 Flood тестирования...")
        self.running = False
        
    def print_header(self):
        print("=" * 60)
        print(f"S3 Flood v{get_version()} - Windows Compatible Version")
        print("=" * 60)
        print()
        
    def ensure_s5cmd(self):
        """Ensure s5cmd is available and working on Windows"""
        tools_dir = Path("tools")
        tools_dir.mkdir(exist_ok=True)
        
        # Try different possible s5cmd locations and names
        possible_paths = [
            tools_dir / "s5cmd.exe",
            tools_dir / "s5cmd",
            Path("s5cmd.exe"),
            Path("s5cmd")
        ]
        
        # Check if any existing s5cmd works
        for s5cmd_path in possible_paths:
            if s5cmd_path.exists():
                try:
                    result = subprocess.run([str(s5cmd_path), "version"], 
                                           capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        print(f"[INFO] Используется существующий s5cmd: {s5cmd_path}")
                        return str(s5cmd_path)
                except Exception:
                    continue
        
        # Try system s5cmd
        try:
            result = subprocess.run(["s5cmd", "version"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print("[INFO] Используется системный s5cmd")
                return "s5cmd"
        except Exception:
            pass
            
        # Download s5cmd for Windows
        print("[INFO] s5cmd не найден или не работает. Загружаю для Windows...")
        return self.download_s5cmd_windows()
        
    def download_s5cmd_windows(self):
        """Download appropriate s5cmd version for Windows"""
        tools_dir = Path("tools")
        tools_dir.mkdir(exist_ok=True)
        
        # Detect Windows architecture
        arch = platform.machine().lower()
        if arch in ['amd64', 'x86_64']:
            s5cmd_arch = "64bit"
        else:
            s5cmd_arch = "32bit"
            
        version = "v2.2.2"  # Latest stable version
        filename = f"s5cmd_{version}_Windows-{s5cmd_arch}.zip"
        url = f"https://github.com/peak/s5cmd/releases/download/{version}/{filename}"
        
        zip_path = tools_dir / filename
        s5cmd_exe = tools_dir / "s5cmd.exe"
        
        try:
            print(f"[INFO] Загружаю s5cmd {version} для Windows {s5cmd_arch}...")
            urllib.request.urlretrieve(url, zip_path)
            
            print("[INFO] Извлекаю s5cmd...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tools_dir)
                
            # Clean up zip file
            zip_path.unlink()
            
            if s5cmd_exe.exists():
                print(f"[SUCCESS] s5cmd установлен: {s5cmd_exe}")
                return str(s5cmd_exe)
            else:
                print("[ERROR] Не удалось извлечь s5cmd")
                return None
                
        except Exception as e:
            print(f"[ERROR] Не удалось загрузить s5cmd: {e}")
            print("[INFO] Вы можете вручную загрузить s5cmd с:")
            print("https://github.com/peak/s5cmd/releases")
            return None
            
    def load_config(self, config_path="config.yaml"):
        """Load configuration from YAML file"""
        default_config = {
            "s3_urls": ["http://localhost:9000"],
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "bucket_name": "test-bucket",
            "cluster_mode": False,
            "parallel_threads": 5,
            "file_groups": {
                "small": {"max_size_mb": 100, "count": 10},
                "medium": {"max_size_mb": 1024, "count": 5},
                "large": {"max_size_mb": 5120, "count": 2}
            },
            "infinite_loop": True,
            "cycle_delay_seconds": 15,
            "test_files_directory": "./s3_temp_files"
        }
        
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
        except FileNotFoundError:
            print(f"Файл конфигурации {config_path} не найден. Создаю конфигурацию по умолчанию.")
            self.config = default_config
            self.save_config(config_path)
            
        # Set local_temp_dir from config
        self.local_temp_dir = Path(self.config["test_files_directory"])
            
    def save_config(self, config_path="config.yaml"):
        """Save configuration to YAML file"""
        try:
            with open(config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            print(f"Конфигурация сохранена в {config_path}")
        except Exception as e:
            print(f"[ERROR] Ошибка сохранения конфигурации: {e}")
            
    def test_s5cmd(self):
        """Test s5cmd connectivity"""
        if not self.s5cmd_path:
            print("[ERROR] s5cmd недоступен")
            print("Пожалуйста, установите s5cmd вручную или проверьте установку.")
            return False
            
        try:
            env = os.environ.copy()
            env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
            env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
            
            cmd = [self.s5cmd_path, "--endpoint-url", self.config["s3_urls"][0], "ls"]
            print(f"[INFO] Выполняю: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
            
            if result.returncode == 0:
                print("[SUCCESS] Проверка S3 соединения прошла успешно")
                if result.stdout.strip():
                    print(f"Вывод S3 листинга:\\n{result.stdout}")
                return True
            else:
                print(f"[ERROR] Проверка S3 соединения не удалась (код выхода {result.returncode})")
                if result.stderr:
                    print(f"Детали ошибки: {result.stderr}")
                if result.stdout:
                    print(f"Вывод: {result.stdout}")
                return False
        except subprocess.TimeoutExpired:
            print("[ERROR] Проверка s5cmd превысила время ожидания (30 секунд)")
            return False
        except Exception as e:
            print(f"[ERROR] Ошибка проверки s5cmd: {e}")
            return False
            
    def create_test_files(self) -> List[Path]:
        """Create test files of different sizes"""
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
        self.local_temp_dir.mkdir(exist_ok=True, parents=True)
        
        file_list = []
        
        print("[INFO] Создаю тестовые файлы...")
        
        # Create small files
        small_config = self.config["file_groups"]["small"]
        for i in range(small_config["count"]):
            size_mb = random.randint(1, small_config["max_size_mb"])
            filename = self.local_temp_dir / f"small_{i}_{size_mb}MB.txt"
            self._create_file(filename, size_mb)
            file_list.append(filename)
            
        # Create medium files
        medium_config = self.config["file_groups"]["medium"]
        for i in range(medium_config["count"]):
            size_mb = random.randint(small_config["max_size_mb"] + 1, medium_config["max_size_mb"])
            filename = self.local_temp_dir / f"medium_{i}_{size_mb}MB.txt"
            self._create_file(filename, size_mb)
            file_list.append(filename)
            
        # Create large files
        large_config = self.config["file_groups"]["large"]
        for i in range(large_config["count"]):
            size_mb = random.randint(medium_config["max_size_mb"] + 1, large_config["max_size_mb"])
            filename = self.local_temp_dir / f"large_{i}_{size_mb}MB.txt"
            self._create_file(filename, size_mb)
            file_list.append(filename)
            
        print(f"[SUCCESS] Создано {len(file_list)} тестовых файлов")
        return file_list
        
    def _create_file(self, filename: Path, size_mb: int):
        """Create a file of specified size in MB"""
        try:
            with open(filename, 'wb') as f:
                # Write in chunks of 1MB
                chunk_size = 1024 * 1024  # 1MB
                chunk_data = b'x' * chunk_size
                
                for _ in range(size_mb):
                    f.write(chunk_data)
        except Exception as e:
            print(f"[ERROR] Не удалось создать файл {filename}: {e}")
            
    def run_s5cmd_upload(self, local_file: Path, bucket_name: str) -> bool:
        """Upload file using s5cmd"""
        if not self.s5cmd_path:
            return False
            
        try:
            env = os.environ.copy()
            env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
            env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
            
            s3_url = self.config["s3_urls"][0]  # Use first URL for simplicity
            s3_path = f"s3://{bucket_name}/{local_file.name}"
            
            cmd = [
                self.s5cmd_path,
                "--endpoint-url", s3_url,
                "cp", str(local_file), s3_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env)
            return result.returncode == 0
            
        except Exception as e:
            print(f"[ERROR] Ошибка загрузки {local_file.name}: {e}")
            return False
            
    def run_test_cycle(self):
        """Run a single test cycle"""
        print("\\n" + "="*50)
        print("Начинаю цикл S3 Flood тестирования")
        print("="*50)
        
        # Create test files
        test_files = self.create_test_files()
        if not test_files:
            print("[ERROR] Не удалось создать тестовые файлы")
            return
            
        # Upload files
        print(f"\\n[INFO] Загружаю {len(test_files)} файлов...")
        upload_start = time.time()
        uploaded_files = []
        
        for file_path in test_files:
            if not self.running:
                break
                
            print(f"Загружаю {file_path.name}...")
            if self.run_s5cmd_upload(file_path, self.config["bucket_name"]):
                uploaded_files.append(file_path)
                self.stats["files_uploaded"] += 1
                self.stats["total_bytes_uploaded"] += file_path.stat().st_size
            else:
                print(f"[ERROR] Не удалось загрузить {file_path.name}")
                
        upload_time = time.time() - upload_start
        self.stats["total_upload_time"] += upload_time
        
        print(f"\\n[SUCCESS] Загружено {len(uploaded_files)} файлов за {upload_time:.2f} секунд")
        
        # Clean up local files
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
            
        self.stats["cycles_completed"] += 1
        print("\\n" + "="*50)
        print("Цикл тестирования завершен!")
        print("="*50)
        
    def show_statistics(self):
        """Display test statistics"""
        print("\\n" + "="*50)
        print("S3 Flood Статистика тестирования")
        print("="*50)
        
        print(f"Завершенных циклов: {self.stats['cycles_completed']}")
        print(f"Загруженных файлов: {self.stats['files_uploaded']}")
        print(f"Скачанных файлов: {self.stats['files_downloaded']}")
        print(f"Удаленных файлов: {self.stats['files_deleted']}")
        print(f"Общее время загрузки: {self.stats['total_upload_time']:.2f} сек")
        print(f"Общее время скачивания: {self.stats['total_download_time']:.2f} сек")
        print(f"Общее время удаления: {self.stats['total_delete_time']:.2f} сек")
        
        # Calculate speeds
        if self.stats['total_upload_time'] > 0:
            upload_speed_mbps = (self.stats['total_bytes_uploaded'] / (1024*1024)) / self.stats['total_upload_time']
            print(f"Средняя скорость загрузки: {upload_speed_mbps:.2f} MB/сек")
            
        if self.stats['total_download_time'] > 0:
            download_speed_mbps = (self.stats['total_bytes_downloaded'] / (1024*1024)) / self.stats['total_download_time']
            print(f"Средняя скорость скачивания: {download_speed_mbps:.2f} MB/сек")
            
        print("="*50)
        
    def interactive_config(self):
        """Simple configuration without rich"""
        print("\\nКонфигурация S3 Flood")
        print("-" * 30)
        
        # S3 URLs
        current_urls = ",".join(self.config.get("s3_urls", ["http://localhost:9000"]))
        urls_input = input(f"S3 Endpoint URLs [{current_urls}]: ").strip()
        if urls_input:
            self.config["s3_urls"] = [url.strip() for url in urls_input.split(",")]
            
        # Access credentials
        access_key = input(f"Access Key [{self.config.get('access_key', 'minioadmin')}]: ").strip()
        if access_key:
            self.config["access_key"] = access_key
            
        secret_key = input(f"Secret Key [{self.config.get('secret_key', 'minioadmin')}]: ").strip()
        if secret_key:
            self.config["secret_key"] = secret_key
            
        # Bucket name
        bucket = input(f"Bucket Name [{self.config.get('bucket_name', 'test-bucket')}]: ").strip()
        if bucket:
            self.config["bucket_name"] = bucket
            
        # Cluster mode
        cluster_choice = input(f"Cluster Mode? (y/n) [{self.config.get('cluster_mode', False)}]: ").strip().lower()
        if cluster_choice in ['y', 'yes', 'д', 'да']:
            self.config["cluster_mode"] = True
        elif cluster_choice in ['n', 'no', 'н', 'нет']:
            self.config["cluster_mode"] = False
            
        # Parallel threads
        try:
            threads_input = input(f"Parallel Threads [{self.config.get('parallel_threads', 5)}]: ").strip()
            if threads_input:
                self.config["parallel_threads"] = int(threads_input)
        except ValueError:
            print("[WARNING] Неверное число потоков, оставляю текущее значение")
            
        # Save configuration
        save_choice = input("Сохранить конфигурацию? (y/n) [y]: ").strip().lower()
        if save_choice not in ['n', 'no', 'н', 'нет']:
            self.save_config()
            
    def run_infinite_loop(self):
        """Run infinite test loop"""
        print("Запуск бесконечного цикла тестирования...")
        print("Нажмите Ctrl+C для остановки")
        
        try:
            while self.running:
                self.run_test_cycle()
                
                if not self.running:
                    break
                    
                # Wait between cycles
                delay = self.config.get("cycle_delay_seconds", 15)
                print(f"\\n[INFO] Жду {delay} секунд до следующего цикла...")
                time.sleep(delay)
                
        except KeyboardInterrupt:
            print("\\n[INFO] Остановка по Ctrl+C...")
            self.running = False
            
    def main_menu(self):
        """Simple main menu"""
        while True:
            self.print_header()
            print("Главное меню:")
            print("1. Проверить S3 соединение")
            print("2. Настроить")
            print("3. Запустить быстрый тест")
            print("4. Запустить бесконечный цикл")
            print("5. Показать статистику")
            print("6. Выход")
            print()
            
            try:
                choice = input("Выберите опцию (1-6): ").strip()
                
                if choice == "1":
                    print("\\nПроверяю S3 соединение...")
                    self.test_s5cmd()
                    input("\\nНажмите Enter для продолжения...")
                    
                elif choice == "2":
                    self.interactive_config()
                    
                elif choice == "3":
                    print("\\n[INFO] Запуск одного цикла тестирования...")
                    self.run_test_cycle()
                    input("\\nНажмите Enter для продолжения...")
                    
                elif choice == "4":
                    self.run_infinite_loop()
                    
                elif choice == "5":
                    self.show_statistics()
                    input("\\nНажмите Enter для продолжения...")
                    
                elif choice == "6":
                    print("\\nДо свидания!")
                    break
                    
                else:
                    print("\\n[ERROR] Неверный выбор. Пожалуйста, введите 1-6.")
                    input("Нажмите Enter для продолжения...")
                    
            except (KeyboardInterrupt, EOFError):
                print("\\n\\nВыход...")
                break
                
def main():
    # Windows console setup
    if platform.system() == "Windows":
        os.system("chcp 65001 >nul")
        os.environ.setdefault("PYTHONIOENCODING", "utf-8")
        
    print("S3 Flood - Windows Compatible Version")
    print("====================================")
    print()
    print("Эта версия полностью совместима с Windows")
    print("и работает без библиотек rich/questionary.")
    print()
    
    tester = WindowsS3FloodTester()
    tester.load_config()
    tester.main_menu()

if __name__ == "__main__":
    main()