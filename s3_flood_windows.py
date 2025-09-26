#!/usr/bin/env python3
"""
S3 Flood - Windows Edition
Единственная Windows версия с полной функциональностью
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

class S3FloodWindows:
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
            "total_bytes_downloaded": 0
        }
        self.running = True
        self.local_temp_dir = Path("./s3_temp_files")
        
        # Register signal handler
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
        except:
            pass
        
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        try:
            print("\\n[INFO] Stopping S3 Flood testing...")
        except:
            pass
        self.running = False
        
    def safe_print(self, message):
        """Safe print that handles console encoding issues"""
        try:
            print(message)
        except:
            try:
                print(message.encode('ascii', 'replace').decode('ascii'))
            except:
                print("Console output error")
        
    def print_header(self):
        try:
            self.safe_print("=" * 60)
            self.safe_print(f"S3 Flood v{get_version()} - Windows Edition")
            self.safe_print("=" * 60)
            self.safe_print("")
        except:
            self.safe_print("S3 Flood - Windows Edition")
            self.safe_print("")
        
    def ensure_s5cmd(self):
        """Ensure s5cmd is available and working"""
        tools_dir = Path("tools")
        tools_dir.mkdir(exist_ok=True)
        
        # Try existing s5cmd
        for s5cmd_path in [tools_dir / "s5cmd.exe", tools_dir / "s5cmd", "s5cmd"]:
            if s5cmd_path.exists() or (isinstance(s5cmd_path, str) and s5cmd_path == "s5cmd"):
                try:
                    test_path = str(s5cmd_path) if s5cmd_path != "s5cmd" else "s5cmd"
                    result = subprocess.run([test_path, "version"], 
                                           capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        self.safe_print(f"[INFO] Using s5cmd: {test_path}")
                        return test_path
                except Exception:
                    continue
        
        # Download s5cmd for Windows
        self.safe_print("[INFO] s5cmd not found. Downloading for Windows...")
        return self.download_s5cmd_windows()
        
    def download_s5cmd_windows(self):
        """Download s5cmd for Windows"""
        tools_dir = Path("tools")
        tools_dir.mkdir(exist_ok=True)
        
        # Detect architecture
        arch = platform.machine().lower()
        s5cmd_arch = "64bit" if arch in ['amd64', 'x86_64'] else "32bit"
            
        version = "v2.2.2"
        filename = f"s5cmd_{version}_Windows-{s5cmd_arch}.zip"
        url = f"https://github.com/peak/s5cmd/releases/download/{version}/{filename}"
        
        zip_path = tools_dir / filename
        s5cmd_exe = tools_dir / "s5cmd.exe"
        
        try:
            self.safe_print(f"[INFO] Downloading s5cmd {version} for Windows {s5cmd_arch}...")
            urllib.request.urlretrieve(url, zip_path)
            
            self.safe_print("[INFO] Extracting s5cmd...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tools_dir)
                
            zip_path.unlink()
            
            if s5cmd_exe.exists():
                self.safe_print(f"[SUCCESS] s5cmd installed: {s5cmd_exe}")
                return str(s5cmd_exe)
            else:
                self.safe_print("[ERROR] Failed to extract s5cmd")
                return None
                
        except Exception as e:
            self.safe_print(f"[ERROR] Failed to download s5cmd: {e}")
            self.safe_print("[INFO] You can manually download s5cmd from:")
            self.safe_print("https://github.com/peak/s5cmd/releases")
            return None
            
    def load_config(self, config_path="config.yaml"):
        """Load configuration"""
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
            self.safe_print(f"Config file {config_path} not found. Creating default config.")
            self.config = default_config
            self.save_config(config_path)
            
        self.local_temp_dir = Path(self.config["test_files_directory"])
            
    def save_config(self, config_path="config.yaml"):
        """Save configuration"""
        try:
            with open(config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            self.safe_print(f"Configuration saved to {config_path}")
        except Exception as e:
            self.safe_print(f"[ERROR] Error saving config: {e}")
            
    def test_s5cmd(self):
        """Test s5cmd connectivity"""
        if not self.s5cmd_path:
            self.safe_print("[ERROR] s5cmd not available")
            return False
            
        try:
            env = os.environ.copy()
            env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
            env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
            
            cmd = [self.s5cmd_path, "--endpoint-url", self.config["s3_urls"][0], "ls"]
            self.safe_print(f"[INFO] Running: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
            
            if result.returncode == 0:
                self.safe_print("[SUCCESS] S3 connection test passed")
                if result.stdout.strip():
                    self.safe_print(f"S3 listing output:\\n{result.stdout}")
                return True
            else:
                self.safe_print(f"[ERROR] S3 connection failed (exit code {result.returncode})")
                if result.stderr:
                    self.safe_print(f"Error details: {result.stderr}")
                return False
        except Exception as e:
            self.safe_print(f"[ERROR] s5cmd test failed: {e}")
            return False
            
    def create_test_files(self) -> List[Path]:
        """Create test files"""
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
        self.local_temp_dir.mkdir(exist_ok=True, parents=True)
        
        file_list = []
        
        self.safe_print("[INFO] Creating test files...")
        
        # Create files from config
        for group_name, group_config in self.config["file_groups"].items():
            for i in range(group_config["count"]):
                size_mb = random.randint(1, group_config["max_size_mb"])
                filename = self.local_temp_dir / f"{group_name}_{i}_{size_mb}MB.txt"
                self._create_file(filename, size_mb)
                file_list.append(filename)
            
        self.safe_print(f"[SUCCESS] Created {len(file_list)} test files")
        return file_list
        
    def _create_file(self, filename: Path, size_mb: int):
        """Create a file of specified size"""
        try:
            with open(filename, 'wb') as f:
                chunk_size = 1024 * 1024  # 1MB
                chunk_data = b'x' * chunk_size
                for _ in range(size_mb):
                    f.write(chunk_data)
        except Exception as e:
            self.safe_print(f"[ERROR] Failed to create file {filename}: {e}")
            
    def run_s5cmd_command(self, operation: str, local_path: str = "", s3_path: str = "") -> bool:
        """Run s5cmd command"""
        if not self.s5cmd_path:
            return False
            
        try:
            env = os.environ.copy()
            env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
            env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
            
            s3_url = self.config["s3_urls"][0]
            
            if operation == "upload":
                cmd = [self.s5cmd_path, "--endpoint-url", s3_url, "cp", local_path, s3_path]
            elif operation == "download":
                null_device = "nul" if platform.system() == "Windows" else "/dev/null"
                cmd = [self.s5cmd_path, "--endpoint-url", s3_url, "cp", s3_path, null_device]
            elif operation == "delete":
                cmd = [self.s5cmd_path, "--endpoint-url", s3_url, "rm", s3_path]
            else:
                return False
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env)
            return result.returncode == 0
            
        except Exception as e:
            self.safe_print(f"[ERROR] s5cmd {operation} failed: {e}")
            return False
            
    def run_test_cycle(self):
        """Run complete test cycle with original logic"""
        self.safe_print("")
        self.safe_print("=" * 50)
        self.safe_print("Starting S3 Flood Test Cycle")
        self.safe_print("=" * 50)
        
        # Step 1: Create test files
        test_files = self.create_test_files()
        if not test_files:
            self.safe_print("[ERROR] Failed to create test files")
            return
        
        # Step 2: Upload ALL files
        self.safe_print(f"\\n[INFO] Uploading ALL {len(test_files)} files to S3...")
        upload_start = time.time()
        uploaded_files = []
        
        for file_path in test_files:
            if not self.running:
                break
                
            s3_path = f"s3://{self.config['bucket_name']}/{file_path.name}"
            self.safe_print(f"Uploading {file_path.name}...")
            
            if self.run_s5cmd_command("upload", str(file_path), s3_path):
                uploaded_files.append(s3_path)
                self.stats["files_uploaded"] += 1
                self.stats["total_bytes_uploaded"] += file_path.stat().st_size
            else:
                self.safe_print(f"[ERROR] Failed to upload {file_path.name}")
                
        upload_time = time.time() - upload_start
        self.stats["total_upload_time"] += upload_time
        self.safe_print(f"[SUCCESS] Uploaded {len(uploaded_files)} files in {upload_time:.2f} seconds")
        
        # Step 3: Concurrent read/write operations in batches
        if uploaded_files and self.running:
            batch_size = self.config["parallel_threads"]
            self.safe_print(f"\\n[INFO] Starting concurrent read/write operations in batches of {batch_size}...")
            
            for i in range(0, len(uploaded_files), batch_size):
                if not self.running:
                    break
                    
                batch_files = uploaded_files[i:i+batch_size]
                
                # Split batch for read/write
                random.shuffle(batch_files)
                mid_point = max(1, len(batch_files) // 2)  # Ensure at least 1 file per operation
                read_files = batch_files[:mid_point]
                write_files = batch_files[mid_point:] if len(batch_files) > 1 else batch_files
                
                batch_num = i//batch_size + 1
                self.safe_print(f"\\nBatch {batch_num}: {len(read_files)} reads, {len(write_files)} writes")
                
                # Read operations
                if read_files:
                    self.safe_print(f"[INFO] Reading {len(read_files)} files...")
                    read_start = time.time()
                    read_success = 0
                    
                    for s3_path in read_files:
                        if not self.running:
                            break
                        file_name = s3_path.split('/')[-1]
                        self.safe_print(f"Reading {file_name}...")
                        if self.run_s5cmd_command("download", "", s3_path):
                            read_success += 1
                            self.stats["files_downloaded"] += 1
                        else:
                            self.safe_print(f"[ERROR] Failed to read {file_name}")
                    
                    read_time = time.time() - read_start
                    self.stats["total_download_time"] += read_time
                    self.safe_print(f"[SUCCESS] Read {read_success} files in {read_time:.2f} seconds")
                
                # Write operations (re-upload)
                if write_files:
                    self.safe_print(f"[INFO] Re-writing {len(write_files)} files...")
                    write_start = time.time()
                    write_success = 0
                    
                    for s3_path in write_files:
                        if not self.running:
                            break
                        
                        file_name = s3_path.split('/')[-1]
                        temp_file = self.local_temp_dir / f"rewrite_{file_name}"
                        
                        # Create temp file
                        try:
                            with open(temp_file, 'wb') as f:
                                f.write(b"Re-write test data " * 1000)  # Small file for re-write
                        except Exception as e:
                            self.safe_print(f"[ERROR] Failed to create temp file: {e}")
                            continue
                        
                        self.safe_print(f"Re-writing {file_name}...")
                        if self.run_s5cmd_command("upload", str(temp_file), s3_path):
                            write_success += 1
                        else:
                            self.safe_print(f"[ERROR] Failed to re-write {file_name}")
                        
                        # Clean up
                        try:
                            temp_file.unlink()
                        except:
                            pass
                    
                    write_time = time.time() - write_start
                    self.safe_print(f"[SUCCESS] Re-wrote {write_success} files in {write_time:.2f} seconds")
        
        # Step 4: Delete all files
        if uploaded_files and self.running:
            self.safe_print(f"\\n[INFO] Deleting ALL {len(uploaded_files)} files from S3...")
            delete_start = time.time()
            delete_success = 0
            
            for s3_path in uploaded_files:
                if not self.running:
                    break
                file_name = s3_path.split('/')[-1]
                self.safe_print(f"Deleting {file_name}...")
                if self.run_s5cmd_command("delete", "", s3_path):
                    delete_success += 1
                    self.stats["files_deleted"] += 1
                else:
                    self.safe_print(f"[ERROR] Failed to delete {file_name}")
            
            delete_time = time.time() - delete_start
            self.stats["total_delete_time"] += delete_time
            self.safe_print(f"[SUCCESS] Deleted {delete_success} files in {delete_time:.2f} seconds")
        
        # Clean up local files
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
            self.safe_print("[INFO] Cleaned up temporary files")
            
        self.stats["cycles_completed"] += 1
        self.safe_print("")
        self.safe_print("=" * 50)
        self.safe_print("Test Cycle Completed!")
        self.safe_print("=" * 50)
        
    def show_statistics(self):
        """Display statistics"""
        self.safe_print("")
        self.safe_print("=" * 50)
        self.safe_print("S3 Flood Test Statistics")
        self.safe_print("=" * 50)
        
        self.safe_print(f"Cycles completed: {self.stats['cycles_completed']}")
        self.safe_print(f"Files uploaded: {self.stats['files_uploaded']}")
        self.safe_print(f"Files downloaded: {self.stats['files_downloaded']}")
        self.safe_print(f"Files deleted: {self.stats['files_deleted']}")
        self.safe_print(f"Total upload time: {self.stats['total_upload_time']:.2f} sec")
        self.safe_print(f"Total download time: {self.stats['total_download_time']:.2f} sec")
        self.safe_print(f"Total delete time: {self.stats['total_delete_time']:.2f} sec")
        
        # Calculate speeds
        if self.stats['total_upload_time'] > 0:
            upload_speed_mbps = (self.stats['total_bytes_uploaded'] / (1024*1024)) / self.stats['total_upload_time']
            self.safe_print(f"Average upload speed: {upload_speed_mbps:.2f} MB/sec")
        
        self.safe_print("=" * 50)
        
    def interactive_config(self):
        """Simple configuration"""
        self.safe_print("")
        self.safe_print("S3 Flood Configuration")
        self.safe_print("-" * 30)
        
        try:
            # S3 URLs
            current_urls = ",".join(self.config.get("s3_urls", ["http://localhost:9000"]))
            urls_input = input(f"S3 Endpoint URLs [{current_urls}]: ").strip()
            if urls_input:
                self.config["s3_urls"] = [url.strip() for url in urls_input.split(",")]
            
            # Credentials
            access_key = input(f"Access Key [{self.config.get('access_key', 'minioadmin')}]: ").strip()
            if access_key:
                self.config["access_key"] = access_key
                
            secret_key = input(f"Secret Key [{self.config.get('secret_key', 'minioadmin')}]: ").strip()
            if secret_key:
                self.config["secret_key"] = secret_key
                
            bucket = input(f"Bucket Name [{self.config.get('bucket_name', 'test-bucket')}]: ").strip()
            if bucket:
                self.config["bucket_name"] = bucket
            
            # Save
            save_choice = input("Save configuration? (y/n) [y]: ").strip().lower()
            if save_choice not in ['n', 'no']:
                self.save_config()
        except:
            pass
            
    def run_infinite_loop(self):
        """Run infinite test loop"""
        self.safe_print("Starting infinite test loop...")
        self.safe_print("Press Ctrl+C to stop")
        
        try:
            while self.running:
                self.run_test_cycle()
                
                if not self.running:
                    break
                    
                delay = self.config.get("cycle_delay_seconds", 15)
                self.safe_print(f"\\n[INFO] Waiting {delay} seconds until next cycle...")
                time.sleep(delay)
                
        except KeyboardInterrupt:
            self.safe_print("\\n[INFO] Stopped by Ctrl+C...")
            self.running = False
            
    def main_menu(self):
        """Main menu"""
        while True:
            self.print_header()
            self.safe_print("Main Menu:")
            self.safe_print("1. Test S3 Connection")
            self.safe_print("2. Configure")
            self.safe_print("3. Run Quick Test")
            self.safe_print("4. Run Infinite Loop")
            self.safe_print("5. Show Statistics")
            self.safe_print("6. Exit")
            self.safe_print("")
            
            try:
                choice = input("Select option (1-6): ").strip()
                
                if choice == "1":
                    self.safe_print("\\nTesting S3 connection...")
                    self.test_s5cmd()
                    input("\\nPress Enter to continue...")
                    
                elif choice == "2":
                    self.interactive_config()
                    
                elif choice == "3":
                    self.safe_print("\\n[INFO] Running single test cycle...")
                    self.run_test_cycle()
                    input("\\nPress Enter to continue...")
                    
                elif choice == "4":
                    self.run_infinite_loop()
                    
                elif choice == "5":
                    self.show_statistics()
                    input("\\nPress Enter to continue...")
                    
                elif choice == "6":
                    self.safe_print("\\nGoodbye!")
                    break
                    
                else:
                    self.safe_print("\\n[ERROR] Invalid choice. Please enter 1-6.")
                    input("Press Enter to continue...")
                    
            except (KeyboardInterrupt, EOFError):
                self.safe_print("\\n\\nExiting...")
                break
                
def main():
    # Windows console setup
    if platform.system() == "Windows":
        try:
            if sys.stdout.encoding.lower() != 'utf-8':
                os.system('chcp 65001 >nul 2>&1')
        except:
            pass
        os.environ.setdefault("PYTHONIOENCODING", "utf-8")
        
    try:
        print("S3 Flood - Windows Edition")
        print("=" * 40)
        print()
        print("Single unified version for Windows compatibility")
        print()
        
        tester = S3FloodWindows()
        tester.load_config()
        tester.main_menu()
        
    except Exception as e:
        print(f"Error starting application: {e}")
        print("Please check your Python installation.")
        try:
            input("Press Enter to exit...")
        except:
            pass

if __name__ == "__main__":
    main()