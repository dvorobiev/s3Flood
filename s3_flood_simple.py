#!/usr/bin/env python3
"""
S3 Flood Simple - Windows compatible version without rich/questionary
"""

import os
import sys
import yaml
import subprocess
import platform
from pathlib import Path
import zipfile
import urllib.request
import urllib.error

def get_version():
    """Get version from VERSION file"""
    try:
        with open(Path(__file__).parent / "VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "development"

class SimpleS3FloodTester:
    def __init__(self):
        self.config = {}
        self.s5cmd_path = self.ensure_s5cmd()
        
    def print_header(self):
        print("="*50)
        print(f"S3 Flood v{get_version()} - Simple Windows Version")
        print("="*50)
        print()
        
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
            print(f"Config file {config_path} not found. Creating default config.")
            self.config = default_config
            self.save_config(config_path)
            
    def save_config(self, config_path="config.yaml"):
        """Save configuration to YAML file"""
        try:
            with open(config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            print(f"Configuration saved to {config_path}")
        except Exception as e:
            print(f"Error saving config: {e}")
            
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
                        print(f"[INFO] Using existing s5cmd: {s5cmd_path}")
                        return str(s5cmd_path)
                except Exception:
                    continue
        
        # Try system s5cmd
        try:
            result = subprocess.run(["s5cmd", "version"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print("[INFO] Using system s5cmd")
                return "s5cmd"
        except Exception:
            pass
            
        # Download s5cmd for Windows
        print("[INFO] s5cmd not found or not working. Downloading for Windows...")
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
            print(f"[INFO] Downloading s5cmd {version} for Windows {s5cmd_arch}...")
            urllib.request.urlretrieve(url, zip_path)
            
            print("[INFO] Extracting s5cmd...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tools_dir)
                
            # Clean up zip file
            zip_path.unlink()
            
            if s5cmd_exe.exists():
                print(f"[SUCCESS] s5cmd installed at {s5cmd_exe}")
                return str(s5cmd_exe)
            else:
                print("[ERROR] s5cmd extraction failed")
                return None
                
        except Exception as e:
            print(f"[ERROR] Failed to download s5cmd: {e}")
            print("[INFO] You can manually download s5cmd from:")
            print("https://github.com/peak/s5cmd/releases")
            return None
            
    def test_s5cmd(self):
        """Test s5cmd connectivity"""
        if not self.s5cmd_path:
            print("[ERROR] s5cmd is not available")
            print("Please manually install s5cmd or check the installation.")
            return False
            
        try:
            env = os.environ.copy()
            env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
            env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
            
            cmd = [self.s5cmd_path, "--endpoint-url", self.config["s3_urls"][0], "ls"]
            print(f"[INFO] Running: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
            
            if result.returncode == 0:
                print("[SUCCESS] S3 connection test passed")
                if result.stdout.strip():
                    print(f"S3 listing output:\n{result.stdout}")
                return True
            else:
                print(f"[ERROR] S3 connection failed (exit code {result.returncode})")
                if result.stderr:
                    print(f"Error details: {result.stderr}")
                if result.stdout:
                    print(f"Output: {result.stdout}")
                return False
        except subprocess.TimeoutExpired:
            print("[ERROR] s5cmd test timed out (30 seconds)")
            return False
        except Exception as e:
            print(f"[ERROR] s5cmd test failed: {e}")
            return False
            
    def interactive_config(self):
        """Simple configuration without rich"""
        print("\nS3 Flood Configuration")
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
        if cluster_choice in ['y', 'yes']:
            self.config["cluster_mode"] = True
        elif cluster_choice in ['n', 'no']:
            self.config["cluster_mode"] = False
            
        # Save configuration
        save_choice = input("Save configuration? (y/n) [y]: ").strip().lower()
        if save_choice != 'n':
            self.save_config()
            
    def main_menu(self):
        """Simple main menu"""
        while True:
            self.print_header()
            print("Main Menu:")
            print("1. Test S3 Connection")
            print("2. Configure")
            print("3. Run Quick Test")
            print("4. Exit")
            print()
            
            try:
                choice = input("Select option (1-4): ").strip()
                
                if choice == "1":
                    print("\nTesting S3 connection...")
                    self.test_s5cmd()
                    input("\nPress Enter to continue...")
                    
                elif choice == "2":
                    self.interactive_config()
                    
                elif choice == "3":
                    print("\n[INFO] Quick test functionality would be implemented here")
                    print("[INFO] This version focuses on configuration and connectivity testing")
                    input("\nPress Enter to continue...")
                    
                elif choice == "4":
                    print("\nGoodbye!")
                    break
                    
                else:
                    print("\n[ERROR] Invalid choice. Please enter 1-4.")
                    input("Press Enter to continue...")
                    
            except (KeyboardInterrupt, EOFError):
                print("\n\nExiting...")
                break
                
def main():
    tester = SimpleS3FloodTester()
    tester.load_config()
    tester.main_menu()

if __name__ == "__main__":
    main()
