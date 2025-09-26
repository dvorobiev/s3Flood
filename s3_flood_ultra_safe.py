#!/usr/bin/env python3
"""
S3 Flood Windows Version - Ultra-compatible version for Windows
This version works without rich/questionary libraries and solves Windows console issues
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
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
        except:
            pass  # Ignore if signal handling fails
        
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        try:
            print("\\n[INFO] Stopping S3 Flood testing...")
        except:
            pass  # Ignore print errors during shutdown
        self.running = False
        
    def safe_print(self, message):
        """Safe print that handles console encoding issues"""
        try:
            print(message)
        except UnicodeEncodeError:
            # Fallback for encoding issues
            print(message.encode('ascii', 'replace').decode('ascii'))
        except:
            # Ultimate fallback
            print("Console output error")
        
    def print_header(self):
        try:
            self.safe_print("=" * 60)
            self.safe_print(f"S3 Flood v{get_version()} - Windows Compatible Version")
            self.safe_print("=" * 60)
            self.safe_print("")
        except:
            # Fallback if console has issues
            self.safe_print("S3 Flood - Windows Compatible Version")
            self.safe_print("")
        
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
                        self.safe_print(f"[INFO] Using existing s5cmd: {s5cmd_path}")
                        return str(s5cmd_path)
                except Exception:
                    continue
        
        # Try system s5cmd
        try:
            result = subprocess.run(["s5cmd", "version"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.safe_print("[INFO] Using system s5cmd")
                return "s5cmd"
        except Exception:
            pass
            
        # Download s5cmd for Windows
        self.safe_print("[INFO] s5cmd not found or not working. Downloading for Windows...")
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
            self.safe_print(f"[INFO] Downloading s5cmd {version} for Windows {s5cmd_arch}...")
            urllib.request.urlretrieve(url, zip_path)
            
            self.safe_print("[INFO] Extracting s5cmd...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tools_dir)
                
            # Clean up zip file
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
            self.safe_print(f"Config file {config_path} not found. Creating default config.")
            self.config = default_config
            self.save_config(config_path)
            
        # Set local_temp_dir from config
        self.local_temp_dir = Path(self.config["test_files_directory"])
            
    def save_config(self, config_path="config.yaml"):
        """Save configuration to YAML file"""
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
            self.safe_print("Please install s5cmd manually or check the installation.")
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
                if result.stdout:
                    self.safe_print(f"Output: {result.stdout}")
                return False
        except subprocess.TimeoutExpired:
            self.safe_print("[ERROR] s5cmd test timed out (30 seconds)")
            return False
        except Exception as e:
            self.safe_print(f"[ERROR] s5cmd test failed: {e}")
            return False
            
    def create_test_files(self) -> List[Path]:
        """Create test files of different sizes"""
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
        self.local_temp_dir.mkdir(exist_ok=True, parents=True)
        
        file_list = []
        
        self.safe_print("[INFO] Creating test files...")
        
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
            
        self.safe_print(f"[SUCCESS] Created {len(file_list)} test files")
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
            self.safe_print(f"[ERROR] Failed to create file {filename}: {e}")
            
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
            self.safe_print(f"[ERROR] Upload error for {local_file.name}: {e}")
            return False
            
    def run_test_cycle(self):
        """Run a single test cycle"""
        self.safe_print("")
        self.safe_print("=" * 50)
        self.safe_print("Starting S3 Flood Test Cycle")
        self.safe_print("=" * 50)
        
        # Create test files
        test_files = self.create_test_files()
        if not test_files:
            self.safe_print("[ERROR] Failed to create test files")
            return
            
        # Upload files
        self.safe_print(f"\\n[INFO] Uploading {len(test_files)} files...")
        upload_start = time.time()
        uploaded_files = []
        
        for file_path in test_files:
            if not self.running:
                break
                
            self.safe_print(f"Uploading {file_path.name}...")
            if self.run_s5cmd_upload(file_path, self.config["bucket_name"]):
                uploaded_files.append(file_path)
                self.stats["files_uploaded"] += 1
                self.stats["total_bytes_uploaded"] += file_path.stat().st_size
            else:
                self.safe_print(f"[ERROR] Failed to upload {file_path.name}")
                
        upload_time = time.time() - upload_start
        self.stats["total_upload_time"] += upload_time
        
        self.safe_print(f"\\n[SUCCESS] Uploaded {len(uploaded_files)} files in {upload_time:.2f} seconds")
        
        # Clean up local files
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
            
        self.stats["cycles_completed"] += 1
        self.safe_print("")
        self.safe_print("=" * 50)
        self.safe_print("Test Cycle Completed!")
        self.safe_print("=" * 50)
        
    def show_statistics(self):
        """Display test statistics"""
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
            
        if self.stats['total_download_time'] > 0:
            download_speed_mbps = (self.stats['total_bytes_downloaded'] / (1024*1024)) / self.stats['total_download_time']
            self.safe_print(f"Average download speed: {download_speed_mbps:.2f} MB/sec")
            
        self.safe_print("=" * 50)
        
    def interactive_config(self):
        """Simple configuration without rich"""
        self.safe_print("")
        self.safe_print("S3 Flood Configuration")
        self.safe_print("-" * 30)
        
        # S3 URLs
        current_urls = ",".join(self.config.get("s3_urls", ["http://localhost:9000"]))
        try:
            urls_input = input(f"S3 Endpoint URLs [{current_urls}]: ").strip()
            if urls_input:
                self.config["s3_urls"] = [url.strip() for url in urls_input.split(",")]
        except:
            pass
            
        # Access credentials
        try:
            access_key = input(f"Access Key [{self.config.get('access_key', 'minioadmin')}]: ").strip()
            if access_key:
                self.config["access_key"] = access_key
        except:
            pass
            
        try:
            secret_key = input(f"Secret Key [{self.config.get('secret_key', 'minioadmin')}]: ").strip()
            if secret_key:
                self.config["secret_key"] = secret_key
        except:
            pass
            
        # Bucket name
        try:
            bucket = input(f"Bucket Name [{self.config.get('bucket_name', 'test-bucket')}]: ").strip()
            if bucket:
                self.config["bucket_name"] = bucket
        except:
            pass
            
        # Cluster mode
        try:
            cluster_choice = input(f"Cluster Mode? (y/n) [{self.config.get('cluster_mode', False)}]: ").strip().lower()
            if cluster_choice in ['y', 'yes']:
                self.config["cluster_mode"] = True
            elif cluster_choice in ['n', 'no']:
                self.config["cluster_mode"] = False
        except:
            pass
            
        # Parallel threads
        try:
            threads_input = input(f"Parallel Threads [{self.config.get('parallel_threads', 5)}]: ").strip()
            if threads_input:
                self.config["parallel_threads"] = int(threads_input)
        except:
            self.safe_print("[WARNING] Invalid thread count, keeping current value")
            
        # Save configuration
        try:
            save_choice = input("Save configuration? (y/n) [y]: ").strip().lower()
            if save_choice not in ['n', 'no']:
                self.save_config()
        except:
            self.save_config()  # Default to saving
            
    def run_infinite_loop(self):
        """Run infinite test loop"""
        self.safe_print("Starting infinite test loop...")
        self.safe_print("Press Ctrl+C to stop")
        
        try:
            while self.running:
                self.run_test_cycle()
                
                if not self.running:
                    break
                    
                # Wait between cycles
                delay = self.config.get("cycle_delay_seconds", 15)
                self.safe_print(f"\\n[INFO] Waiting {delay} seconds until next cycle...")
                time.sleep(delay)
                
        except KeyboardInterrupt:
            self.safe_print("\\n[INFO] Stopped by Ctrl+C...")
            self.running = False
            
    def main_menu(self):
        """Simple main menu"""
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
    # Windows console setup - simplified and safer
    if platform.system() == "Windows":
        try:
            # Set console to UTF-8 only if needed
            if sys.stdout.encoding.lower() != 'utf-8':
                os.system('chcp 65001 >nul 2>&1')
        except:
            pass  # Ignore encoding setup errors
        
        # Basic environment setup
        os.environ.setdefault("PYTHONIOENCODING", "utf-8")
        
    # Use simple print statements without special characters
    try:
        print("S3 Flood - Windows Compatible Version")
        print("=" * 40)
        print()
        print("This version is fully compatible with Windows")
        print("and works without rich/questionary libraries.")
        print()
        
        tester = WindowsS3FloodTester()
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