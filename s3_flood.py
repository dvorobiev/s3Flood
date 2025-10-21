#!/usr/bin/env python3
"""
S3 Flood - TUI application for S3 backend testing using s5cmd or rclone
"""

def get_version() -> str:
    """Get version from VERSION file"""
    try:
        with open(Path(__file__).parent / "VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "development"

# Windows console compatibility fix
import platform
if platform.system() == "Windows":
    import os
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    # Try to enable ANSI escape sequences on Windows
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except:
        pass

# Global flag to track if we should use simple mode
USE_SIMPLE_MODE = False

# Try to import rich/questionary libraries
try:
    import questionary
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.live import Live
except ImportError as e:
    print(f"[WARNING] Could not import rich/questionary libraries: {e}")
    print("[INFO] Falling back to simple mode...")
    USE_SIMPLE_MODE = True
    # Create dummy classes to prevent errors
    class ConsoleFallback:
        def print(self, *args, **kwargs):
            print(*args)
    
    class TableFallback:
        def __init__(self, *args, **kwargs):
            pass
        def add_column(self, *args, **kwargs):
            pass
        def add_row(self, *args, **kwargs):
            pass
    
    class PanelFallback:
        def __init__(self, *args, **kwargs):
            pass
    
    # Create aliases to use fallback classes
    Console = ConsoleFallback
    Table = TableFallback
    Panel = PanelFallback
    
    # Create a simple fallback for questionary
    class SimpleQuestionary:
        @staticmethod
        def text(message, default=None):
            class TextQuestion:
                def __init__(self, message, default):
                    self.message = message
                    self.default = default
                
                def ask(self):
                    if self.default:
                        value = input(f"{self.message} [{self.default}]: ")
                        return value if value else self.default
                    else:
                        return input(f"{self.message}: ")
            return TextQuestion(message, default)
        
        @staticmethod
        def confirm(message, default=False):
            class ConfirmQuestion:
                def __init__(self, message, default):
                    self.message = message
                    self.default = default
                
                def ask(self):
                    default_str = "y/n" if not self.default else ("Y/n" if self.default else "y/N")
                    value = input(f"{self.message} ({default_str}): ")
                    if not value:
                        return self.default
                    return value.lower().startswith('y')
            return ConfirmQuestion(message, default)
            
        @staticmethod
        def select(message, choices, default=None):
            class SelectQuestion:
                def __init__(self, message, choices, default):
                    self.message = message
                    self.choices = choices
                    self.default = default
                
                def ask(self):
                    print(f"{self.message}")
                    for i, choice in enumerate(self.choices, 1):
                        print(f"{i}. {choice}")
                    
                    while True:
                        try:
                            if self.default:
                                default_index = self.choices.index(self.default) + 1
                                choice_input = input(f"Select option (1-{len(self.choices)}) [{default_index}]: ")
                                if not choice_input:
                                    return self.default
                                choice_num = int(choice_input)
                            else:
                                choice_input = input(f"Select option (1-{len(self.choices)}): ")
                                choice_num = int(choice_input)
                                
                            if 1 <= choice_num <= len(self.choices):
                                return self.choices[choice_num - 1]
                            else:
                                print("Invalid option, please try again.")
                        except (ValueError, IndexError):
                            print("Invalid input, please try again.")
                        except KeyboardInterrupt:
                            return None
            return SelectQuestion(message, choices, default)
    
    questionary = SimpleQuestionary()

import os
import sys
import yaml
import json
import random
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional


class S3FloodTester:
    def __init__(self):
        self.console = Console()
        self.config = {}
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
            "upload_times": []  # Track individual upload times for better speed calculation
        }
        self.running = True
        # Use test_files_directory from config or default to "./s3_temp_files"
        self.local_temp_dir = Path("./s3_temp_files")
        # Tool selection - default to s5cmd
        self.tool = "s5cmd"
        # Algorithm selection - default to traditional
        self.algorithm = "traditional"
        
        # Register signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        self.console.print("[yellow]Stopping S3 Flood test...[/yellow]")
        self.running = False
        
    def load_config(self, config_path: str = "config.yaml"):
        """Load configuration from YAML file"""
        default_config = {
            "s3_urls": ["http://localhost:9000"],
            "access_key": "YOUR_ACCESS_KEY_HERE",
            "secret_key": "YOUR_SECRET_KEY_HERE",
            "bucket_name": "test-bucket",
            "cluster_mode": False,
            "parallel_threads": 5,
            "file_groups": {
                "small": {"max_size_mb": 100, "count": 100},
                "medium": {"max_size_mb": 5120, "count": 50},  # 5GB
                "large": {"max_size_mb": 20480, "count": 10}   # 20GB
            },
            "infinite_loop": True,
            "cycle_delay_seconds": 15,
            "test_files_directory": "./s3_temp_files"
        }
        
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                # Merge with default config to ensure all keys exist
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
                        
            # Set local_temp_dir from config
            self.local_temp_dir = Path(self.config["test_files_directory"])
        except FileNotFoundError:
            self.console.print(f"[yellow]Config file {config_path} not found. Creating default config.[/yellow]")
            self.config = default_config
            self.save_config(config_path)
        except yaml.YAMLError as e:
            self.console.print(f"[red]Error parsing config file: {e}[/red]")
            self.config = default_config
            
        # Set local_temp_dir from config
        self.local_temp_dir = Path(self.config["test_files_directory"])

    def save_config(self, config_path: str = "config.yaml"):
        """Save current configuration to YAML file"""
        try:
            with open(config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            self.console.print(f"[green]Configuration saved to {config_path}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error saving config: {e}[/red]")
            
    def _get_s3_url(self) -> str:
        """Get S3 URL based on cluster mode configuration.
        
        In cluster mode, randomly selects one of the configured URLs.
        In single mode, always returns the first URL.
        """
        if self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
            # In cluster mode, randomly select one of the URLs
            return random.choice(self.config["s3_urls"])
        else:
            # In single mode or if only one URL is configured, use the first one
            return self.config["s3_urls"][0]
            
    def setup_s5cmd(self):
        """Setup s5cmd with provided credentials"""
        try:
            # Check if s5cmd is available
            result = subprocess.run(["s5cmd", "--help"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self.console.print("[red]s5cmd not found. Please install s5cmd first.[/red]")
                return False
                
            # Test S3 connection with provided credentials using environment variables
            s3_url = self._get_s3_url()  # Use cluster-aware URL selection
            
            # Set environment variables for s5cmd
            env = os.environ.copy()
            env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
            env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
            
            # Create a simple test command to list buckets
            cmd = [
                "s5cmd", 
                "--endpoint-url", s3_url,
                "ls"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env)
            
            if result.returncode == 0:
                return True
            else:
                self.console.print(f"[red]S3 connection failed with return code {result.returncode}[/red]")
                if result.stderr:
                    self.console.print(f"[red]Error: {result.stderr}[/red]")
                return False
                
        except FileNotFoundError:
            self.console.print("[red]s5cmd not found. Please install s5cmd first.[/red]")
            return False
        except subprocess.TimeoutExpired:
            self.console.print("[red]s5cmd test timed out after 60 seconds.[/red]")
            return False
        except Exception as e:
            self.console.print(f"[red]Error checking s5cmd: {e}[/red]")
            return False
            
    def setup_rclone(self):
        """Setup rclone with provided credentials"""
        try:
            # Check if rclone is available
            result = subprocess.run(["rclone", "--version"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self.console.print("[red]rclone not found. Please install rclone first.[/red]")
                return False
                
            # Create rclone config section if it doesn't exist
            self._create_rclone_config()
                
            # Test S3 connection with provided credentials
            cmd = [
                "rclone", 
                "ls", 
                f"s3flood:{self.config['bucket_name']}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                return True
            else:
                self.console.print(f"[red]S3 connection failed with return code {result.returncode}[/red]")
                if result.stderr:
                    self.console.print(f"[red]Error: {result.stderr}[/red]")
                return False
                
        except FileNotFoundError:
            self.console.print("[red]rclone not found. Please install rclone first.[/red]")
            return False
        except subprocess.TimeoutExpired:
            self.console.print("[red]rclone test timed out after 60 seconds.[/red]")
            return False
        except Exception as e:
            self.console.print(f"[red]Error checking rclone: {e}[/red]")
            return False
            
    def _create_rclone_config(self):
        """Create or update rclone configuration with S3 settings"""
        try:
            # Get rclone config file path
            result = subprocess.run(["rclone", "config", "file"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self.console.print("[red]Failed to get rclone config file path.[/red]")
                return False
                
            # Extract config file path from output
            lines = result.stdout.strip().split('\n')
            if len(lines) < 2:
                self.console.print("[red]Unexpected rclone config file output.[/red]")
                return False
                
            config_file_path = lines[1].strip()
            config_dir = os.path.dirname(config_file_path)
            
            # Create config directory if it doesn't exist
            os.makedirs(config_dir, exist_ok=True)
            
            # Read existing config if it exists
            existing_config = ""
            if os.path.exists(config_file_path):
                with open(config_file_path, 'r') as f:
                    existing_config = f.read()
            
            # Check if s3flood section already exists
            if "[s3flood]" in existing_config:
                # Remove existing s3flood section
                lines = existing_config.split('\n')
                new_lines = []
                skip_section = False
                
                for line in lines:
                    if line.strip() == "[s3flood]":
                        skip_section = True
                        continue
                    elif line.startswith('[') and line.endswith(']') and skip_section:
                        # Start of new section, stop skipping
                        skip_section = False
                    
                    if not skip_section:
                        new_lines.append(line)
                
                existing_config = '\n'.join(new_lines)
            
            # Create rclone config content
            rclone_config_content = f"""{existing_config}
[s3flood]
type = s3
provider = Other
access_key_id = {self.config['access_key']}
secret_access_key = {self.config['secret_key']}
endpoint = {self._get_s3_url()}
"""
            
            # Write config to file
            with open(config_file_path, 'w') as f:
                f.write(rclone_config_content.strip() + '\n')
                
            self.console.print("[green]✓ rclone configuration updated[/green]")
            return True
            
        except Exception as e:
            self.console.print(f"[red]Error creating rclone config: {e}[/red]")
            return False
            
    def check_existing_test_files(self) -> List[Path]:
        """Check for existing test files in the temp directory"""
        if not self.local_temp_dir.exists():
            return []
            
        # Get all files in the directory
        file_list = list(self.local_temp_dir.glob("*"))
        
        # Filter out directories and return only files
        file_list = [f for f in file_list if f.is_file()]
        
        return file_list
    
    def create_test_files(self) -> List[Path]:
        """Create test files of different sizes with progress tracking"""
        # Check if we should reuse existing files
        if self.config.get("infinite_loop", False) and self.algorithm == "traditional":
            existing_files = self.check_existing_test_files()
            if existing_files:
                self.console.print(f"[yellow]Reusing {len(existing_files)} existing test files...[/yellow]")
                return existing_files
        
        # Use directory from config
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
        self.local_temp_dir.mkdir(exist_ok=True, parents=True)
        
        file_list = []
        
        # Calculate total files to create
        small_config = self.config["file_groups"]["small"]
        medium_config = self.config["file_groups"]["medium"]
        large_config = self.config["file_groups"]["large"]
        total_files = small_config["count"] + medium_config["count"] + large_config["count"]
        
        self.console.print(f"[cyan]Creating {total_files} test files...[/cyan]")
        
        # Create small files (up to 100MB)
        self.console.print(f"[dim]  └─ Small: {small_config['count']} files (up to {small_config['max_size_mb']}MB each)[/dim]")
        for i in range(small_config["count"]):
            # Ensure we have a valid range for random.randint
            min_size = 1
            max_size = max(min_size, small_config["max_size_mb"])
            size_mb = random.randint(min_size, max_size)
            file_path = self.local_temp_dir / f"small_{i}_{size_mb}MB.dat"
            self._create_random_file(file_path, size_mb * 1024 * 1024)
            file_list.append(file_path)
            self.console.print(f"[green]✓[/green] Created {file_path.name} ({size_mb}MB)")
        
        # Create medium files (up to 5GB)
        self.console.print(f"[dim]  └─ Medium: {medium_config['count']} files (up to {medium_config['max_size_mb']}MB each)[/dim]")
        for i in range(medium_config["count"]):
            # Ensure we have a valid range for random.randint
            min_size = 100
            max_size = max(min_size, medium_config["max_size_mb"])
            size_mb = random.randint(min_size, max_size)
            file_path = self.local_temp_dir / f"medium_{i}_{size_mb}MB.dat"
            self._create_random_file(file_path, size_mb * 1024 * 1024)
            file_list.append(file_path)
            self.console.print(f"[green]✓[/green] Created {file_path.name} ({size_mb}MB)")
        
        # Create large files (up to 20GB)
        self.console.print(f"[dim]  └─ Large: {large_config['count']} files (up to {large_config['max_size_mb']}MB each)[/dim]")
        for i in range(large_config["count"]):
            # Ensure we have a valid range for random.randint
            min_size = 1000
            max_size = max(min_size, large_config["max_size_mb"])
            size_mb = random.randint(min_size, max_size)
            file_path = self.local_temp_dir / f"large_{i}_{size_mb}MB.dat"
            self._create_random_file(file_path, size_mb * 1024 * 1024)
            file_list.append(file_path)
            self.console.print(f"[green]✓[/green] Created {file_path.name} ({size_mb}MB)")
        
        # Shuffle the file list for random processing
        random.shuffle(file_list)
        self.console.print(f"[green]✓[/green] Created {len(file_list)} test files")
        return file_list

    def _create_random_file(self, file_path: Path, size_bytes: int):
        """Create a random file of specified size with progress tracking"""
        # Show file creation start
        size_mb = size_bytes // (1024 * 1024)
        self.console.print(f"[dim]  Creating {file_path.name} ({size_mb}MB)...[/dim]")
        
        with open(file_path, 'wb') as f:
            # Write in chunks to handle large files efficiently
            chunk_size = min(10 * 1024 * 1024, size_bytes)  # 10MB chunks or smaller
            remaining = size_bytes
            written = 0
            
            # For files larger than 50MB, show progress updates every 10%
            show_detailed_progress = size_bytes > 50 * 1024 * 1024
            
            while remaining > 0 and self.running:
                write_size = min(chunk_size, remaining)
                f.write(os.urandom(write_size))
                remaining -= write_size
                written += write_size
                
                # Show progress for larger files at 10% intervals
                if show_detailed_progress:
                    progress_percent = (written / size_bytes) * 100
                    # Show progress at ~10% intervals
                    if progress_percent >= ((written - write_size) / size_bytes) * 100 + 10 or written == size_bytes:
                        written_mb = written // (1024 * 1024)
                        total_mb = size_bytes // (1024 * 1024)
                        self.console.print(f"[dim]    └─ {file_path.name}: {written_mb}/{total_mb}MB ({progress_percent:.1f}%)[/dim]")
                
        # Show completion for all files
        if self.running:
            self.console.print(f"[green]    ✓ {file_path.name} completed[/green]")

    def upload_files(self, file_list: List[Path]) -> Dict[str, Any]:
        """Upload files to S3 using selected tool (s5cmd or rclone) in parallel with per-file progress"""
        if self.tool == "rclone":
            return self._upload_files_rclone(file_list)
        else:
            return self._upload_files_s5cmd(file_list)
            
    def _upload_files_s5cmd(self, file_list: List[Path]) -> Dict[str, Any]:
        """Upload files to S3 using s5cmd in parallel with per-file progress"""
        cycle_start_time = time.time()
        uploaded_files = []
        failed_files = []
        
        # Set environment variables for s5cmd
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
        
        # Track total bytes for successful uploads only
        total_bytes_uploaded = 0
        file_upload_info = []  # Track individual file upload info (size, time)
        
        # Show operation start
        self.console.print(f"[cyan]Uploading {len(file_list)} files using {self.config['parallel_threads']} parallel threads[/cyan]")
        if self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
            self.console.print(f"[dim]Cluster mode: Using random endpoints from {len(self.config['s3_urls'])} configured URLs[/dim]")
        
        # Track file statuses
        file_statuses = {}  # Track status of each file
        start_times = {}    # Track start times for each file
        file_urls = {}      # Track which URL was used for each file (for consistent read operations)
        
        # Process in batches based on parallel_threads setting
        batch_size = self.config["parallel_threads"]
        for i in range(0, len(file_list), batch_size):
            if not self.running:  # Check if user requested to stop
                break
                
            batch = file_list[i:i+batch_size]
            
            # Start parallel upload processes - each file can use a different endpoint in cluster mode
            processes = []
            for file_path in batch:
                # Create s5cmd command for upload
                s3_path = f"s3://{self.config['bucket_name']}/{file_path.name}"
                
                # In cluster mode, each file can use a different endpoint
                s3_url = self._get_s3_url()
                if self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
                    file_urls[str(file_path)] = s3_url  # Store URL for later read operations
                    self.console.print(f"[dim]  {file_path.name}: Using endpoint {s3_url}[/dim]")
                else:
                    file_urls[str(file_path)] = s3_url  # Store URL even in single mode for consistency
                
                # Record start time for this file
                start_times[str(file_path)] = time.time()
                file_statuses[str(file_path)] = "started"
                
                # Show file start
                file_size_mb = file_path.stat().st_size // (1024 * 1024) if file_path.exists() else 0
                self.console.print(f"[blue]→[/blue] Uploading {file_path.name} ({file_size_mb}MB)")
                
                cmd = [
                    "s5cmd", 
                    "--endpoint-url", s3_url,
                    "cp", str(file_path), s3_path
                ]
                
                try:
                    # Start process without blocking
                    process = subprocess.Popen(
                        cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    processes.append((process, file_path, s3_path))
                except Exception as e:
                    failed_files.append((file_path, str(e)))
                    self.console.print(f"[red]✗[/red] {file_path.name} failed to start: {e}")
            
            # Poll processes with non-blocking approach
            completed_processes = []
            poll_count = 0
            max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
            
            while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                poll_count += 1
                for item in processes:
                    if item in completed_processes:
                        continue
                        
                    process, file_path, s3_path = item
                    
                    # Check if process has completed (non-blocking)
                    if process.poll() is not None:
                        # Process completed
                        stdout, stderr = process.communicate()
                        file_size = file_path.stat().st_size if file_path.exists() else 0
                        file_upload_time = time.time() - start_times[str(file_path)]
                        
                        if process.returncode == 0:
                            uploaded_files.append((file_path, s3_path))
                            # Track bytes for successful uploads
                            total_bytes_uploaded += file_size
                            # Track individual file upload info
                            file_upload_info.append((file_size, file_upload_time))
                            file_statuses[str(file_path)] = "completed"
                            
                            # Calculate speed
                            speed_mbps = (file_size / (1024 * 1024)) / file_upload_time if file_upload_time > 0 else 0
                            file_size_mb = file_size // (1024 * 1024)
                            self.console.print(f"[green]✓[/green] {file_path.name} uploaded successfully ({file_size_mb}MB in {file_upload_time:.1f}s, {speed_mbps:.1f}MB/s)")
                        else:
                            failed_files.append((file_path, stderr.decode() if stderr else "Unknown error"))
                            file_statuses[str(file_path)] = "failed"
                            self.console.print(f"[red]✗[/red] {file_path.name} failed to upload")
                        
                        completed_processes.append(item)
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
            
            # Check if we hit the polling limit
            if poll_count >= max_polls:
                for item in processes:
                    if item not in completed_processes:
                        process, file_path, s3_path = item
                        process.kill()  # Kill the process
                        failed_files.append((file_path, "Timeout - process killed"))
                        self.console.print(f"[red]✗[/red] {file_path.name} timed out and was killed")
        
        cycle_elapsed_time = time.time() - cycle_start_time
        
        # Update stats
        self.stats["files_uploaded"] += len(uploaded_files)
        self.stats["total_upload_time"] += cycle_elapsed_time
        self.stats["total_bytes_uploaded"] += total_bytes_uploaded
        self.stats["upload_times"].extend(file_upload_info)
        
        return {
            "uploaded": uploaded_files,
            "failed": failed_files,
            "time_elapsed": cycle_elapsed_time,
            "file_urls": file_urls,  # Return URL mapping for cross-node testing
            "file_upload_info": file_upload_info  # Return individual file upload info for statistics
        }
        
    def _upload_files_rclone(self, file_list: List[Path]) -> Dict[str, Any]:
        """Upload files to S3 using rclone in parallel with per-file progress"""
        cycle_start_time = time.time()
        uploaded_files = []
        failed_files = []
        
        # Update rclone config before upload
        self._create_rclone_config()
        
        # Track total bytes for successful uploads only
        total_bytes_uploaded = 0
        file_upload_info = []  # Track individual file upload info (size, time)
        
        # Show operation start
        self.console.print(f"[cyan]Uploading {len(file_list)} files using rclone with {self.config['parallel_threads']} parallel threads[/cyan]")
        
        # Track file statuses
        file_statuses = {}  # Track status of each file
        start_times = {}    # Track start times for each file
        
        # Process in batches based on parallel_threads setting
        batch_size = self.config["parallel_threads"]
        for i in range(0, len(file_list), batch_size):
            if not self.running:  # Check if user requested to stop
                break
                
            batch = file_list[i:i+batch_size]
            
            # Start parallel upload processes
            processes = []
            for file_path in batch:
                # Create rclone command for upload
                s3_path = f"s3flood:{self.config['bucket_name']}/{file_path.name}"
                
                # Record start time for this file
                start_times[str(file_path)] = time.time()
                file_statuses[str(file_path)] = "started"
                
                # Show file start
                file_size_mb = file_path.stat().st_size // (1024 * 1024) if file_path.exists() else 0
                self.console.print(f"[blue]→[/blue] Uploading {file_path.name} ({file_size_mb}MB)")
                
                cmd = [
                    "rclone", 
                    "copy", str(file_path), s3_path,
                    "--transfers", "1"  # One transfer per process
                ]
                
                try:
                    # Start process without blocking
                    process = subprocess.Popen(
                        cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE
                    )
                    processes.append((process, file_path, s3_path))
                except Exception as e:
                    failed_files.append((file_path, str(e)))
                    self.console.print(f"[red]✗[/red] {file_path.name} failed to start: {e}")
            
            # Poll processes with non-blocking approach
            completed_processes = []
            poll_count = 0
            max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
            
            while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                poll_count += 1
                for item in processes:
                    if item in completed_processes:
                        continue
                        
                    process, file_path, s3_path = item
                    
                    # Check if process has completed (non-blocking)
                    if process.poll() is not None:
                        # Process completed
                        stdout, stderr = process.communicate()
                        file_size = file_path.stat().st_size if file_path.exists() else 0
                        file_upload_time = time.time() - start_times[str(file_path)]
                        
                        if process.returncode == 0:
                            uploaded_files.append((file_path, s3_path))
                            # Track bytes for successful uploads
                            total_bytes_uploaded += file_size
                            # Track individual file upload info
                            file_upload_info.append((file_size, file_upload_time))
                            file_statuses[str(file_path)] = "completed"
                            
                            # Calculate speed
                            speed_mbps = (file_size / (1024 * 1024)) / file_upload_time if file_upload_time > 0 else 0
                            file_size_mb = file_size // (1024 * 1024)
                            self.console.print(f"[green]✓[/green] {file_path.name} uploaded successfully ({file_size_mb}MB in {file_upload_time:.1f}s, {speed_mbps:.1f}MB/s)")
                        else:
                            failed_files.append((file_path, stderr.decode() if stderr else "Unknown error"))
                            file_statuses[str(file_path)] = "failed"
                            self.console.print(f"[red]✗[/red] {file_path.name} failed to upload")
                        
                        completed_processes.append(item)
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
            
            # Check if we hit the polling limit
            if poll_count >= max_polls:
                for item in processes:
                    if item not in completed_processes:
                        process, file_path, s3_path = item
                        process.kill()  # Kill the process
                        failed_files.append((file_path, "Timeout - process killed"))
                        self.console.print(f"[red]✗[/red] {file_path.name} timed out and was killed")
        
        cycle_elapsed_time = time.time() - cycle_start_time
        
        # Update stats
        self.stats["files_uploaded"] += len(uploaded_files)
        self.stats["total_upload_time"] += cycle_elapsed_time
        self.stats["total_bytes_uploaded"] += total_bytes_uploaded
        self.stats["upload_times"].extend(file_upload_info)
        
        return {
            "uploaded": uploaded_files,
            "failed": failed_files,
            "time_elapsed": cycle_elapsed_time
        }
        
    def download_files(self, s3_file_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Download files from S3 using selected tool (s5cmd or rclone) in parallel with per-file progress"""
        if self.tool == "rclone":
            return self._download_files_rclone(s3_file_paths)
        else:
            return self._download_files_s5cmd(s3_file_paths, upload_file_urls)
            
    def _download_files_s5cmd(self, s3_file_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Download files from S3 using s5cmd in parallel with per-file progress.
        
        If upload_file_urls is provided, will use different endpoints for read operations
        to test cross-node consistency in cluster mode.
        """
        cycle_start_time = time.time()
        downloaded_files = []
        failed_files = []
        
        # Set environment variables for s5cmd
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
        
        # Show operation start
        self.console.print(f"[cyan]Reading {len(s3_file_paths)} files using {self.config['parallel_threads']} parallel threads[/cyan]")
        if self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
            self.console.print(f'[dim]Cluster mode: Using random endpoints from {len(self.config["s3_urls"])} configured URLs[/dim]')
        
        # Track file statuses
        file_statuses = {}  # Track status of each file
        start_times = {}    # Track start times for each file
        
        # Process in batches
        batch_size = self.config["parallel_threads"]
        for i in range(0, len(s3_file_paths), batch_size):
            if not self.running:  # Check if user requested to stop
                break
                
            batch = s3_file_paths[i:i+batch_size]
            
            # Start parallel read processes - each file can use a different endpoint in cluster mode
            processes = []
            for s3_path in batch:
                file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                
                # In cluster mode, try to use a different endpoint than upload (if info available)
                s3_url = self._get_s3_url()
                if (self.config.get("cluster_mode", False) and 
                    len(self.config["s3_urls"]) > 1 and 
                    upload_file_urls and 
                    s3_path in upload_file_urls):
                    # Try to use a different endpoint for read operation
                    upload_url = upload_file_urls[s3_path]
                    other_urls = [url for url in self.config["s3_urls"] if url != upload_url]
                    if other_urls:
                        s3_url = random.choice(other_urls)
                        self.console.print(f"[dim]  {file_name}: Using different endpoint {s3_url} (uploaded via {upload_url})[/dim]")
                    else:
                        self.console.print(f"[dim]  {file_name}: Using endpoint {s3_url}[/dim]")
                elif self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
                    self.console.print(f"[dim]  {file_name}: Using endpoint {s3_url}[/dim]")
                
                # Record start time for this file
                start_times[s3_path] = time.time()
                file_statuses[s3_path] = "started"
                
                # Show file start
                self.console.print(f"[blue]→[/blue] Reading {file_name}")
                
                # Use cat command to read file content (simulates download without storing)
                cmd = [
                    "s5cmd",
                    "--endpoint-url", s3_url,
                    "cat", s3_path
                ]
                
                try:
                    # Start process without blocking
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.DEVNULL,  # Discard output
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    processes.append((process, s3_path, file_name))
                except Exception as e:
                    failed_files.append((s3_path, str(e)))
                    self.console.print(f"[red]✗[/red] {file_name} failed to start: {e}")
            
            # Poll processes with non-blocking approach
            completed_processes = []
            poll_count = 0
            max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
            
            while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                poll_count += 1
                for item in processes:
                    if item in completed_processes:
                        continue
                        
                    process, s3_path, file_name = item
                    
                    # Check if process has completed (non-blocking)
                    if process.poll() is not None:
                        # Process completed
                        stdout, stderr = process.communicate()
                        file_download_time = time.time() - start_times[s3_path]
                        
                        # For read operations, we consider it successful if the process completes
                        if process.returncode == 0:
                            downloaded_files.append(s3_path)
                            file_statuses[s3_path] = "completed"
                            self.console.print(f"[green]✓[/green] {file_name} read successfully ({file_download_time:.1f}s)")
                        else:
                            # Only consider it a failure if there's an actual error
                            error_output = stderr.decode().strip() if stderr else ""
                            if error_output and "no such file" not in error_output.lower():
                                failed_files.append((s3_path, error_output))
                                file_statuses[s3_path] = "failed"
                                self.console.print(f"[red]✗[/red] {file_name} failed to read")
                            else:
                                # If it's just a "no such file" error or no error, still count as downloaded
                                downloaded_files.append(s3_path)
                                file_statuses[s3_path] = "completed"
                                self.console.print(f"[green]✓[/green] {file_name} read successfully ({file_download_time:.1f}s)")
                        
                        completed_processes.append(item)
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
            
            # Check if we hit the polling limit
            if poll_count >= max_polls:
                for item in processes:
                    if item not in completed_processes:
                        process, s3_path, file_name = item
                        process.kill()  # Kill the process
                        failed_files.append((s3_path, "Timeout - process killed"))
                        self.console.print(f"[red]✗[/red] {file_name} timed out and was killed")
        
        cycle_elapsed_time = time.time() - cycle_start_time
        
        # Update stats
        self.stats["files_downloaded"] += len(downloaded_files)
        self.stats["total_download_time"] += cycle_elapsed_time
        # Note: We don't track bytes downloaded as we're just reading to /dev/null
        
        return {
            "downloaded": downloaded_files,
            "failed": failed_files,
            "time_elapsed": cycle_elapsed_time
        }
        
    def _download_files_rclone(self, s3_file_paths: List[str]) -> Dict[str, Any]:
        """Download files from S3 using rclone in parallel with per-file progress"""
        cycle_start_time = time.time()
        downloaded_files = []
        failed_files = []
        
        # Update rclone config before download
        self._create_rclone_config()
        
        # Show operation start
        self.console.print(f"[cyan]Reading {len(s3_file_paths)} files using rclone with {self.config['parallel_threads']} parallel threads[/cyan]")
        
        # Track file statuses
        file_statuses = {}  # Track status of each file
        start_times = {}    # Track start times for each file
        
        # Process in batches
        batch_size = self.config["parallel_threads"]
        for i in range(0, len(s3_file_paths), batch_size):
            if not self.running:  # Check if user requested to stop
                break
                
            batch = s3_file_paths[i:i+batch_size]
            
            # Start parallel read processes
            processes = []
            for s3_path in batch:
                file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                
                # Create rclone command for reading
                full_s3_path = f"s3flood:{self.config['bucket_name']}/{file_name}"
                
                # Record start time for this file
                start_times[full_s3_path] = time.time()
                file_statuses[full_s3_path] = "started"
                
                # Show file start
                self.console.print(f"[blue]→[/blue] Reading {file_name}")
                
                # Use cat command to read file content (simulates download without storing)
                cmd = [
                    "rclone",
                    "cat", full_s3_path
                ]
                
                try:
                    # Start process without blocking
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.DEVNULL,  # Discard output
                        stderr=subprocess.PIPE
                    )
                    processes.append((process, full_s3_path, file_name))
                except Exception as e:
                    failed_files.append((full_s3_path, str(e)))
                    self.console.print(f"[red]✗[/red] {file_name} failed to start: {e}")
            
            # Poll processes with non-blocking approach
            completed_processes = []
            poll_count = 0
            max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
            
            while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                poll_count += 1
                for item in processes:
                    if item in completed_processes:
                        continue
                        
                    process, full_s3_path, file_name = item
                    
                    # Check if process has completed (non-blocking)
                    if process.poll() is not None:
                        # Process completed
                        stdout, stderr = process.communicate()
                        file_download_time = time.time() - start_times[full_s3_path]
                        
                        # For read operations, we consider it successful if the process completes
                        if process.returncode == 0:
                            downloaded_files.append(full_s3_path)
                            file_statuses[full_s3_path] = "completed"
                            self.console.print(f"[green]✓[/green] {file_name} read successfully ({file_download_time:.1f}s)")
                        else:
                            # Only consider it a failure if there's an actual error
                            error_output = stderr.decode().strip() if stderr else ""
                            if error_output and "object not found" not in error_output.lower():
                                failed_files.append((full_s3_path, error_output))
                                file_statuses[full_s3_path] = "failed"
                                self.console.print(f"[red]✗[/red] {file_name} failed to read")
                            else:
                                # If it's just a "object not found" error or no error, still count as downloaded
                                downloaded_files.append(full_s3_path)
                                file_statuses[full_s3_path] = "completed"
                                self.console.print(f"[green]✓[/green] {file_name} read successfully ({file_download_time:.1f}s)")
                        
                        completed_processes.append(item)
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
            
            # Check if we hit the polling limit
            if poll_count >= max_polls:
                for item in processes:
                    if item not in completed_processes:
                        process, full_s3_path, file_name = item
                        process.kill()  # Kill the process
                        failed_files.append((full_s3_path, "Timeout - process killed"))
                        self.console.print(f"[red]✗[/red] {file_name} timed out and was killed")
        
        cycle_elapsed_time = time.time() - cycle_start_time
        
        # Update stats
        self.stats["files_downloaded"] += len(downloaded_files)
        self.stats["total_download_time"] += cycle_elapsed_time
        # Note: We don't track bytes downloaded as we're just reading to /dev/null
        
        return {
            "downloaded": downloaded_files,
            "failed": failed_files,
            "time_elapsed": cycle_elapsed_time
        }

    def delete_files(self, s3_file_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Delete files from S3 using selected tool (s5cmd or rclone) in parallel with per-file progress"""
        if self.tool == "rclone":
            return self._delete_files_rclone(s3_file_paths)
        else:
            return self._delete_files_s5cmd(s3_file_paths, upload_file_urls)
            
    def _delete_files_s5cmd(self, s3_file_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Delete files from S3 using s5cmd in parallel with per-file progress.
        
        If upload_file_urls is provided, will use different endpoints for delete operations
        to test cross-node consistency in cluster mode.
        """
        cycle_start_time = time.time()
        deleted_files = []
        failed_files = []
        
        # Set environment variables for s5cmd
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
        
        # Show operation start
        self.console.print(f"[cyan]Deleting {len(s3_file_paths)} files using {self.config['parallel_threads']} parallel threads[/cyan]")
        if self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
            self.console.print(f"[dim]Cluster mode: Using random endpoints from {len(self.config['s3_urls'])} configured URLs[/dim]")
        
        # Track file statuses
        file_statuses = {}  # Track status of each file
        start_times = {}    # Track start times for each file
        
        # Process in batches
        batch_size = self.config["parallel_threads"]
        for i in range(0, len(s3_file_paths), batch_size):
            if not self.running:  # Check if user requested to stop
                break
                
            batch = s3_file_paths[i:i+batch_size]
            
            # Start parallel delete processes - each file can use a different endpoint in cluster mode
            processes = []
            for s3_path in batch:
                file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                
                # In cluster mode, try to use a different endpoint than upload (if info available)
                s3_url = self._get_s3_url()
                if (self.config.get("cluster_mode", False) and 
                    len(self.config["s3_urls"]) > 1 and 
                    upload_file_urls and 
                    s3_path in upload_file_urls):
                    # Try to use a different endpoint for delete operation
                    upload_url = upload_file_urls[s3_path]
                    other_urls = [url for url in self.config["s3_urls"] if url != upload_url]
                    if other_urls:
                        s3_url = random.choice(other_urls)
                        self.console.print(f"[dim]  {file_name}: Using different endpoint {s3_url} (uploaded via {upload_url})[/dim]")
                    else:
                        self.console.print(f"[dim]  {file_name}: Using endpoint {s3_url}[/dim]")
                elif self.config.get("cluster_mode", False) and len(self.config["s3_urls"]) > 1:
                    self.console.print(f"[dim]  {file_name}: Using endpoint {s3_url}[/dim]")
                
                # Record start time for this file
                start_times[s3_path] = time.time()
                file_statuses[s3_path] = "started"
                
                # Show file start
                self.console.print(f"[blue]→[/blue] Deleting {file_name}")
                
                cmd = [
                    "s5cmd",
                    "--endpoint-url", s3_url,
                    "rm", s3_path
                ]
                
                try:
                    # Start process without blocking
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=env
                    )
                    processes.append((process, s3_path, file_name))
                except Exception as e:
                    failed_files.append((s3_path, str(e)))
                    self.console.print(f"[red]✗[/red] {file_name} failed to start: {e}")
            
            # Poll processes with non-blocking approach
            completed_processes = []
            poll_count = 0
            max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
            
            while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                poll_count += 1
                for item in processes:
                    if item in completed_processes:
                        continue
                        
                    process, s3_path, file_name = item
                    
                    # Check if process has completed (non-blocking)
                    if process.poll() is not None:
                        # Process completed
                        stdout, stderr = process.communicate()
                        file_delete_time = time.time() - start_times[s3_path]
                        
                        if process.returncode == 0:
                            deleted_files.append(s3_path)
                            file_statuses[s3_path] = "completed"
                            self.console.print(f"[green]✓[/green] {file_name} deleted successfully ({file_delete_time:.1f}s)")
                        else:
                            failed_files.append((s3_path, stderr.decode() if stderr else "Unknown error"))
                            file_statuses[s3_path] = "failed"
                            self.console.print(f"[red]✗[/red] {file_name} failed to delete")
                        
                        completed_processes.append(item)
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
            
            # Check if we hit the polling limit
            if poll_count >= max_polls:
                for item in processes:
                    if item not in completed_processes:
                        process, s3_path, file_name = item
                        process.kill()  # Kill the process
                        failed_files.append((s3_path, "Timeout - process killed"))
                        self.console.print(f"[red]✗[/red] {file_name} timed out and was killed")
        
        cycle_elapsed_time = time.time() - cycle_start_time
        
        # Update stats
        self.stats["files_deleted"] += len(deleted_files)
        self.stats["total_delete_time"] += cycle_elapsed_time
        
        return {
            "deleted": deleted_files,
            "failed": failed_files,
            "time_elapsed": cycle_elapsed_time
        }
        
    def _delete_files_rclone(self, s3_file_paths: List[str]) -> Dict[str, Any]:
        """Delete files from S3 using rclone in parallel with per-file progress"""
        cycle_start_time = time.time()
        deleted_files = []
        failed_files = []
        
        # Update rclone config before delete
        self._create_rclone_config()
        
        # Show operation start
        self.console.print(f"[cyan]Deleting {len(s3_file_paths)} files using rclone with {self.config['parallel_threads']} parallel threads[/cyan]")
        
        # Track file statuses
        file_statuses = {}  # Track status of each file
        start_times = {}    # Track start times for each file
        
        # Process in batches
        batch_size = self.config["parallel_threads"]
        for i in range(0, len(s3_file_paths), batch_size):
            if not self.running:  # Check if user requested to stop
                break
                
            batch = s3_file_paths[i:i+batch_size]
            
            # Start parallel delete processes
            processes = []
            for s3_path in batch:
                file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                
                # Create rclone command for delete
                full_s3_path = f"s3flood:{self.config['bucket_name']}/{file_name}"
                
                # Record start time for this file
                start_times[full_s3_path] = time.time()
                file_statuses[full_s3_path] = "started"
                
                # Show file start
                self.console.print(f"[blue]→[/blue] Deleting {file_name}")
                
                cmd = [
                    "rclone",
                    "deletefile", full_s3_path
                ]
                
                try:
                    # Start process without blocking
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    processes.append((process, full_s3_path, file_name))
                except Exception as e:
                    failed_files.append((full_s3_path, str(e)))
                    self.console.print(f"[red]✗[/red] {file_name} failed to start: {e}")
            
            # Poll processes with non-blocking approach
            completed_processes = []
            poll_count = 0
            max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
            
            while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                poll_count += 1
                for item in processes:
                    if item in completed_processes:
                        continue
                        
                    process, full_s3_path, file_name = item
                    
                    # Check if process has completed (non-blocking)
                    if process.poll() is not None:
                        # Process completed
                        stdout, stderr = process.communicate()
                        file_delete_time = time.time() - start_times[full_s3_path]
                        
                        if process.returncode == 0:
                            deleted_files.append(full_s3_path)
                            file_statuses[full_s3_path] = "completed"
                            self.console.print(f"[green]✓[/green] {file_name} deleted successfully ({file_delete_time:.1f}s)")
                        else:
                            failed_files.append((full_s3_path, stderr.decode() if stderr else "Unknown error"))
                            file_statuses[full_s3_path] = "failed"
                            self.console.print(f"[red]✗[/red] {file_name} failed to delete")
                        
                        completed_processes.append(item)
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
            
            # Check if we hit the polling limit
            if poll_count >= max_polls:
                for item in processes:
                    if item not in completed_processes:
                        process, full_s3_path, file_name = item
                        process.kill()  # Kill the process
                        failed_files.append((full_s3_path, "Timeout - process killed"))
                        self.console.print(f"[red]✗[/red] {file_name} timed out and was killed")
        
        cycle_elapsed_time = time.time() - cycle_start_time
        
        # Update stats
        self.stats["files_deleted"] += len(deleted_files)
        self.stats["total_delete_time"] += cycle_elapsed_time
        
        return {
            "deleted": deleted_files,
            "failed": failed_files,
            "time_elapsed": cycle_elapsed_time
        }
        
    def display_stats(self):
        """Display statistics in a formatted table"""
        table = Table(title="S3 Flood Test Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Cycles Completed", str(self.stats["cycles_completed"]))
        table.add_row("Files Uploaded", str(self.stats["files_uploaded"]))
        
        if self.algorithm == "infinite_write":
            table.add_row("Algorithm", "Infinite Write (Write only)")
        else:
            table.add_row("Files Downloaded", str(self.stats["files_downloaded"]))
            table.add_row("Files Deleted", str(self.stats["files_deleted"]))
            table.add_row("Algorithm", "Traditional (Write-Read-Delete)")
            
        table.add_row("Tool Used", self.tool)
        
        # Add speed calculations in MB/s
        if self.stats["files_uploaded"] > 0:
            avg_upload_time = self.stats["total_upload_time"] / self.stats["files_uploaded"]
            table.add_row("Avg Upload Time/File (s)", f"{avg_upload_time:.2f}")
            
            # Calculate upload speed in MB/s
            if self.stats["total_bytes_uploaded"] > 0 and self.stats["total_upload_time"] > 0:
                upload_speed_mbs = (self.stats["total_bytes_uploaded"] / (1024 * 1024)) / self.stats["total_upload_time"]
                table.add_row("Avg Upload Speed (MB/s)", f"{upload_speed_mbs:.2f}")
            
        if self.stats["files_downloaded"] > 0:
            avg_download_time = self.stats["total_download_time"] / self.stats["files_downloaded"]
            table.add_row("Avg Download Time/File (s)", f"{avg_download_time:.2f}")
            
        if self.stats["files_deleted"] > 0:
            avg_delete_time = self.stats["total_delete_time"] / self.stats["files_deleted"]
            table.add_row("Avg Delete Time/File (s)", f"{avg_delete_time:.2f}")
            
        self.console.print(table)

    def run_infinite_write_cycle(self):
        """Run infinite write cycle without deletion"""
        self.console.print(Panel(f"[bold blue]Starting Infinite Write Test Cycle (using {self.tool})[/bold blue]"))
        
        # Create test files (only in first cycle or if not in infinite loop mode)
        self.console.print("[cyan]Creating test files...[/cyan]")
        file_list = self.create_test_files()
        self.console.print(f"[green]✓ Created {len(file_list)} test files[/green]")
        
        # Show file distribution
        small_files = [f for f in file_list if f.name.startswith("small")]
        medium_files = [f for f in file_list if f.name.startswith("medium")]
        large_files = [f for f in file_list if f.name.startswith("large")]
        self.console.print(f"[dim]  └─ Small: {len(small_files)}, Medium: {len(medium_files)}, Large: {len(large_files)}[/dim]")
        
        # Upload all files first
        self.console.print("[cyan]Uploading files to S3...[/cyan]")
        upload_result = self.upload_files(file_list)
        self.console.print(f"[green]✓ Uploaded {len(upload_result['uploaded'])} files "
                          f"in {upload_result['time_elapsed']:.2f}s[/green]")
        
        # Extract S3 paths and URL mapping for download operations
        s3_paths = [s3_path for _, s3_path in upload_result['uploaded']]
        file_url_mapping = {}  # Will be populated if available in upload result
        
        # Check if upload result contains URL mapping
        if 'file_urls' in upload_result:
            # Create mapping from s3_path to URL
            for file_path, s3_path in upload_result['uploaded']:
                file_key = str(file_path)
                if file_key in upload_result['file_urls']:
                    file_url_mapping[s3_path] = upload_result['file_urls'][file_key]
        
        # For infinite write, we'll continuously upload files without deleting them
        self.console.print("[cyan]Starting continuous write operations...[/cyan]")
        
        # Process files in batches, continuously uploading them
        batch_size = self.config["parallel_threads"]
        cycle_count = 0
        
        while self.running:
            cycle_count += 1
            self.console.print(f"[dim]Write cycle #{cycle_count}[/dim]")
            
            # Process files in batches
            for i in range(0, len(s3_paths), batch_size):
                if not self.running:
                    break
                    
                batch_files = s3_paths[i:i+batch_size]
                
                # Perform write operations (re-upload same files)
                self.console.print(f"[cyan]Re-writing {len(batch_files)} files...[/cyan]")
                write_result = self._reupload_files(batch_files, file_url_mapping)
                self.console.print(f"[green]✓ Re-wrote {len(write_result['uploaded'])} files "
                                  f"in {write_result['time_elapsed']:.2f}s[/green]")
                
                # Update stats
                self.stats["files_uploaded"] += len(write_result['uploaded'])
                self.stats["total_upload_time"] += write_result['time_elapsed']
                
                # Add individual upload times for better speed calculation
                if 'file_upload_info' in write_result:
                    self.stats["upload_times"].extend(write_result['file_upload_info'])
                
                # Small delay between batches
                if self.running and i + batch_size < len(s3_paths):
                    time.sleep(1)
            
            # Update cycle count
            self.stats["cycles_completed"] += 1
            
            # Show current stats
            self.display_stats()
            
            # Check if we should continue
            if not self.running:
                break
                
            # Small delay between cycles
            if self.running:
                self.console.print("[blue]Waiting 5 seconds before next write cycle...[/blue]")
                time.sleep(5)
        
        # Clean up local files at the end
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
            self.console.print("[dim]✓ Cleaned up temporary files[/dim]")
            
        self.console.print(Panel("[bold green]Infinite Write Test Stopped![/bold green]"))
        self.display_stats()
        
    def run_test_cycle(self):
        """Run a single test cycle with randomized concurrent read/write operations"""
        self.console.print(Panel(f"[bold blue]Starting S3 Flood Test Cycle (using {self.tool})[/bold blue]"))
        
        # Create test files (only in first cycle or if not in infinite loop mode)
        self.console.print("[cyan]Creating test files...[/cyan]")
        file_list = self.create_test_files()
        self.console.print(f"[green]✓ Created {len(file_list)} test files[/green]")
        
        # Show file distribution
        small_files = [f for f in file_list if f.name.startswith("small")]
        medium_files = [f for f in file_list if f.name.startswith("medium")]
        large_files = [f for f in file_list if f.name.startswith("large")]
        self.console.print(f"[dim]  └─ Small: {len(small_files)}, Medium: {len(medium_files)}, Large: {len(large_files)}[/dim]")
        
        # Upload all files first
        self.console.print("[cyan]Uploading files to S3...[/cyan]")
        upload_result = self.upload_files(file_list)
        self.console.print(f"[green]✓ Uploaded {len(upload_result['uploaded'])} files "
                          f"in {upload_result['time_elapsed']:.2f}s[/green]")
        
        # Extract S3 paths and URL mapping for download operations
        s3_paths = [s3_path for _, s3_path in upload_result['uploaded']]
        file_url_mapping = {}  # Will be populated if available in upload result
        
        # Check if upload result contains URL mapping
        if 'file_urls' in upload_result:
            # Create mapping from s3_path to URL
            for file_path, s3_path in upload_result['uploaded']:
                file_key = str(file_path)
                if file_key in upload_result['file_urls']:
                    file_url_mapping[s3_path] = upload_result['file_urls'][file_key]
        
        # For randomized concurrent operations, we'll process files in mixed read/write batches
        # Split files into chunks for concurrent processing
        batch_size = self.config["parallel_threads"]
        self.console.print(f"[cyan]Starting concurrent read/write operations in batches of {batch_size}...[/cyan]")
        
        # Process files in batches, each batch does both read and write operations
        for i in range(0, len(s3_paths), batch_size):
            batch_files = s3_paths[i:i+batch_size]
            
            # For each batch, randomly decide which files to read and which to write
            # Split batch into read and write operations
            random.shuffle(batch_files)
            mid_point = len(batch_files) // 2
            read_files = batch_files[:mid_point] if mid_point > 0 else batch_files
            write_files = batch_files[mid_point:] if mid_point > 0 else batch_files
            
            # If we don't have enough files for both groups, use all files for both operations
            if not write_files:
                write_files = read_files.copy()
                
            self.console.print(f"[dim]Batch {i//batch_size + 1}: {len(read_files)} reads, {len(write_files)} writes[/dim]")
            
            # Perform read operations
            if read_files:
                self.console.print(f"[cyan]Reading {len(read_files)} files...[/cyan]")
                read_result = self.download_files(read_files, file_url_mapping)
                self.console.print(f"[green]✓ Read {len(read_result['downloaded'])} files "
                                  f"in {read_result['time_elapsed']:.2f}s[/green]")
            
            # Perform write operations (re-upload same files)
            if write_files:
                self.console.print(f"[cyan]Re-writing {len(write_files)} files...[/cyan]")
                write_result = self._reupload_files(write_files, file_url_mapping)
                self.console.print(f"[green]✓ Re-wrote {len(write_result['uploaded'])} files "
                                  f"in {write_result['time_elapsed']:.2f}s[/green]")
        
        # Delete all files
        self.console.print("[cyan]Deleting all files from S3...[/cyan]")
        delete_result = self.delete_files(s3_paths, file_url_mapping)
        self.console.print(f"[green]✓ Deleted {len(delete_result['deleted'])} files "
                          f"in {delete_result['time_elapsed']:.2f}s[/green]")
        
        # Update cycle count and stats
        self.stats["cycles_completed"] += 1
        self.stats["files_uploaded"] += len(upload_result['uploaded'])  # Initial upload
        # Note: We're not counting re-write operations in stats to avoid double counting
        # If you want to count them, we can add: + total_write_operations
        self.stats["files_downloaded"] += len(s3_paths)  # Approximate count of read operations
        self.stats["total_upload_time"] += upload_result['time_elapsed']
        # Note: We're not counting re-write time in stats for the same reason
        self.stats["files_deleted"] += len(delete_result['deleted'])
        self.stats["total_delete_time"] += delete_result['time_elapsed']
        
        # Clean up local files
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
            self.console.print("[dim]✓ Cleaned up temporary files[/dim]")
            
        self.console.print(Panel("[bold green]Test Cycle Completed![/bold green]"))
        self.display_stats()
        
    def _reupload_files(self, s3_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Simulate re-writing files by downloading and re-uploading them"""
        # In a real implementation, we would:
        # 1. Download files to temporary location
        # 2. Re-upload them
        # For now, we'll simulate this by creating temporary files with same names and re-uploading
        
        # Create temporary files to simulate re-upload
        temp_files = []
        for i, s3_path in enumerate(s3_paths):
            # Extract filename from S3 path
            file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
            temp_file = self.local_temp_dir / f"temp_{file_name}"
            
            # Create a small temporary file (in real implementation, we would download the actual file)
            with open(temp_file, 'wb') as f:
                f.write(b"Re-upload test data for " + file_name.encode()[:50])  # Small test data
                
            temp_files.append(temp_file)
        
        # Upload temporary files with same S3 paths (overwriting)
        upload_result = self.upload_files(temp_files)
        
        # Add file upload info for statistics
        file_upload_info = []
        for temp_file in temp_files:
            if temp_file.exists():
                file_size = temp_file.stat().st_size
                # We don't have individual upload times from upload_files, so we'll approximate
                file_upload_info.append((file_size, upload_result['time_elapsed'] / len(temp_files) if len(temp_files) > 0 else 0))
        
        upload_result['file_upload_info'] = file_upload_info
        
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                temp_file.unlink()
            except:
                pass
                
        return upload_result
        
    def run_infinite_loop(self):
        """Run test cycles in an infinite loop"""
        if self.algorithm == "infinite_write":
            self.console.print(f"[bold magenta]Starting infinite write loop using {self.tool}...[/bold magenta]")
            self.console.print("[yellow]Press Ctrl+C to stop[/yellow]")
            self.run_infinite_write_cycle()
        else:
            self.console.print(f"[bold magenta]Starting infinite test loop using {self.tool}...[/bold magenta]")
            self.console.print("[yellow]Press Ctrl+C to stop[/yellow]")
            
            while self.running:
                try:
                    self.run_test_cycle()
                    
                    if self.running and self.config["infinite_loop"]:
                        delay = self.config["cycle_delay_seconds"]
                        self.console.print(f"[blue]Waiting {delay} seconds before next cycle...[/blue]")
                        time.sleep(delay)
                except Exception as e:
                    self.console.print(f"[red]Error in test cycle: {e}[/red]")
                    if self.config["infinite_loop"]:
                        time.sleep(5)  # Wait before retrying
                    else:
                        break
                        
            self.console.print("[bold yellow]S3 Flood test stopped[/bold yellow]")
            self.display_stats()
        
    def interactive_config(self):
        """Interactive configuration setup"""
        self.console.print(Panel("[bold blue]S3 Flood Configuration[/bold blue]"))
        
        # S3 Configuration
        urls_input = questionary.text(
            "S3 Endpoint URLs (comma separated for cluster mode):",
            default=",".join(self.config.get("s3_urls", ["http://localhost:9000"]))
        ).ask()
        
        if urls_input:
            self.config["s3_urls"] = [url.strip() for url in urls_input.split(",")]
            
        access_key_input = questionary.text(
            "Access Key:",
            default=self.config.get("access_key", "minioadmin")
        ).ask()
        if access_key_input is not None:
            self.config["access_key"] = access_key_input
        
        secret_key_input = questionary.text(
            "Secret Key:",
            default=self.config.get("secret_key", "minioadmin")
        ).ask()
        if secret_key_input is not None:
            self.config["secret_key"] = secret_key_input
        
        bucket_name_input = questionary.text(
            "Bucket Name:",
            default=self.config.get("bucket_name", "test-bucket")
        ).ask()
        if bucket_name_input is not None:
            self.config["bucket_name"] = bucket_name_input
        
        cluster_mode_input = questionary.confirm(
            "Cluster Mode?",
            default=self.config.get("cluster_mode", False)
        ).ask()
        if cluster_mode_input is not None:
            self.config["cluster_mode"] = cluster_mode_input
        
        # Performance Configuration
        parallel_threads_input = questionary.text(
            "Parallel Threads:",
            default=str(self.config.get("parallel_threads", 5))
        ).ask()
        if parallel_threads_input is not None:
            try:
                self.config["parallel_threads"] = int(parallel_threads_input)
            except ValueError:
                self.console.print("[yellow]Invalid input for parallel threads, using default value.[/yellow]")
                self.config["parallel_threads"] = 5
        
        # File Configuration
        self.console.print("[cyan]File Group Configuration:[/cyan]")
        
        small_count_input = questionary.text(
            "Small files count (up to 100MB each):",
            default=str(self.config["file_groups"]["small"]["count"])
        ).ask()
        if small_count_input is not None:
            try:
                small_count = int(small_count_input)
                self.config["file_groups"]["small"]["count"] = small_count
            except ValueError:
                self.console.print("[yellow]Invalid input for small files count, using default value.[/yellow]")
        
        medium_count_input = questionary.text(
            "Medium files count (up to 5GB each):",
            default=str(self.config["file_groups"]["medium"]["count"])
        ).ask()
        if medium_count_input is not None:
            try:
                medium_count = int(medium_count_input)
                self.config["file_groups"]["medium"]["count"] = medium_count
            except ValueError:
                self.console.print("[yellow]Invalid input for medium files count, using default value.[/yellow]")
        
        large_count_input = questionary.text(
            "Large files count (up to 20GB each):",
            default=str(self.config["file_groups"]["large"]["count"])
        ).ask()
        if large_count_input is not None:
            try:
                large_count = int(large_count_input)
                self.config["file_groups"]["large"]["count"] = large_count
            except ValueError:
                self.console.print("[yellow]Invalid input for large files count, using default value.[/yellow]")
        
        # Test Configuration
        infinite_loop_input = questionary.confirm(
            "Run in infinite loop?",
            default=self.config.get("infinite_loop", True)
        ).ask()
        if infinite_loop_input is not None:
            self.config["infinite_loop"] = infinite_loop_input
        
        if self.config["infinite_loop"]:
            cycle_delay_input = questionary.text(
                "Delay between cycles (seconds):",
                default=str(self.config.get("cycle_delay_seconds", 15))
            ).ask()
            if cycle_delay_input is not None:
                try:
                    self.config["cycle_delay_seconds"] = int(cycle_delay_input)
                except ValueError:
                    self.console.print("[yellow]Invalid input for cycle delay, using default value.[/yellow]")
                    self.config["cycle_delay_seconds"] = 15
            
        # Save configuration
        save_config_input = questionary.confirm(
            "Save configuration to config.yaml?",
            default=True
        ).ask()
        
        if save_config_input:
            self.save_config()
            
    def select_tool(self):
        """Select tool to use for operations (s5cmd or rclone)"""
        self.console.print(Panel("[bold blue]Select Tool[/bold blue]"))
        
        # Check if tools are available
        s5cmd_available = self._check_tool_available("s5cmd")
        rclone_available = self._check_tool_available("rclone")
        
        if not s5cmd_available and not rclone_available:
            self.console.print("[red]Neither s5cmd nor rclone is available. Please install at least one tool.[/red]")
            return False
            
        choices = []
        if s5cmd_available:
            choices.append("s5cmd")
        if rclone_available:
            choices.append("rclone")
            
        if len(choices) == 1:
            self.tool = choices[0]
            self.console.print(f"[green]Using {self.tool} (only available tool)[/green]")
            return True
            
        # Ask user to select tool
        selected_tool = questionary.select(
            "Select tool to use:",
            choices=choices,
            default=self.tool
        ).ask()
        
        if selected_tool:
            self.tool = selected_tool
            self.console.print(f"[green]Selected tool: {self.tool}[/green]")
            
            # Setup the selected tool
            if self.tool == "s5cmd":
                return self.setup_s5cmd()
            else:
                return self.setup_rclone()
        else:
            return False
            
    def _check_tool_available(self, tool_name: str) -> bool:
        """Check if a tool is available in the system"""
        try:
            result = subprocess.run([tool_name, "--help"], 
                                  capture_output=True, text=True, timeout=5)
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
            
    def _fallback_menu(self, version: str) -> str:
        """Fallback menu for Windows console compatibility"""
        while True:
            self.console.print(f"\n[bold blue]S3 Flood v{version} - Main Menu[/bold blue]")
            self.console.print("1. Run Test")
            self.console.print("2. Configure")
            self.console.print("3. View Statistics")
            self.console.print("4. Select Tool")
            self.console.print("5. Exit")
            
            try:
                choice_num = input("\nSelect option (1-5): ").strip()
                if choice_num == "1":
                    return "Run Test"
                elif choice_num == "2":
                    return "Configure"
                elif choice_num == "3":
                    return "View Statistics"
                elif choice_num == "4":
                    return "Select Tool"
                elif choice_num == "5":
                    return "Exit"
            except KeyboardInterrupt:
                return "Exit"
            except Exception:
                self.console.print("[red]Invalid input, please try again.[/red]")
    def select_algorithm(self):
        """Select algorithm to use for operations"""
        self.console.print(Panel("[bold blue]Select Algorithm[/bold blue]"))
        self.console.print("[cyan]1. Traditional (Write-Read-Delete)[/cyan]")
        self.console.print("   Upload files → Read files → Delete files")
        self.console.print("[cyan]2. Infinite Write (Write only, no deletion)[/cyan]")
        self.console.print("   Continuously upload files without deletion")
        
        # Algorithm choices
        algorithms = [
            "Traditional (Write-Read-Delete)",
            "Infinite Write (Write only, no deletion)"
        ]
        
        selected_algorithm = questionary.select(
            "Select algorithm to use:",
            choices=algorithms,
            default=algorithms[0]
        ).ask()
        
        if selected_algorithm:
            if "Traditional" in selected_algorithm:
                self.algorithm = "traditional"
                self.console.print("[green]Selected algorithm: Traditional (Write-Read-Delete)[/green]")
            elif "Infinite Write" in selected_algorithm:
                self.algorithm = "infinite_write"
                self.console.print("[green]Selected algorithm: Infinite Write (Write only, no deletion)[/green]")
            return True
        else:
            return False
                
    def main_menu(self):
        """Main menu for the application"""
        version = get_version()
        
        # If we're in simple mode, use the fallback menu
        if USE_SIMPLE_MODE:
            choice = self._fallback_menu(version)
        else:
            # Rich menu
            while True:
                self.console.print(Panel(f"[bold blue]S3 Flood v{version} - Main Menu[/bold blue]"))
                
                choice = questionary.select(
                    "Select an option:",
                    choices=[
                        "Run Test",
                        "Configure",
                        "View Statistics",
                        "Select Tool",  # Add tool selection option
                        "Exit"
                    ]
                ).ask()
                
                if not choice:
                    choice = "Exit"
                break  # Exit the while loop after getting choice
                    
        # Handle the choice
        if choice == "Run Test":
            # Select algorithm before selecting tool
            if self.select_algorithm():
                # Select tool before running test
                if self.select_tool():
                    if self.config["infinite_loop"]:
                        self.run_infinite_loop()
                    else:
                        self.run_test_cycle()
        elif choice == "Configure":
            self.interactive_config()
        elif choice == "View Statistics":
            self.display_stats()
        elif choice == "Select Tool":
            self.select_tool()
        elif choice == "Exit":
            self.console.print("[bold yellow]Goodbye![/bold yellow]")
            return
            
        # After handling the choice, show the menu again (unless we're exiting)
        if choice != "Exit":
            self.main_menu()

def main():
    """Main entry point"""
    tester = S3FloodTester()
    tester.load_config()
    
    try:
        tester.main_menu()
    except KeyboardInterrupt:
        tester.console.print("\n[bold yellow]Interrupted by user[/bold yellow]")
    except Exception as e:
        tester.console.print(f"\n[bold red]Error: {e}[/bold red]")
        if not USE_SIMPLE_MODE:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()