#!/usr/bin/env python3
"""
S3 Flood - TUI application for S3 backend testing using s5cmd
"""

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
from typing import List, Dict, Any

import questionary
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live


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
        self.local_temp_dir = Path("./s3_temp_files")
        
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
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "bucket_name": "test-bucket",
            "cluster_mode": False,
            "parallel_threads": 5,
            "file_groups": {
                "small": {"max_size_mb": 100, "count": 100},
                "medium": {"max_size_mb": 5120, "count": 50},  # 5GB
                "large": {"max_size_mb": 20480, "count": 10}   # 20GB
            },
            "infinite_loop": True,
            "cycle_delay_seconds": 15
        }
        
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                # Merge with default config to ensure all keys exist
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
        except FileNotFoundError:
            self.console.print(f"[yellow]Config file {config_path} not found. Creating default config.[/yellow]")
            self.config = default_config
            self.save_config(config_path)
        except yaml.YAMLError as e:
            self.console.print(f"[red]Error parsing config file: {e}[/red]")
            self.config = default_config
            
    def save_config(self, config_path: str = "config.yaml"):
        """Save current configuration to YAML file"""
        try:
            with open(config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            self.console.print(f"[green]Configuration saved to {config_path}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error saving config: {e}[/red]")
            
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
            s3_url = self.config["s3_urls"][0]
            
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
            
    def create_test_files(self) -> List[Path]:
        """Create test files of different sizes"""
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
        self.local_temp_dir.mkdir(exist_ok=True)
        
        file_list = []
        
        # Create small files (up to 100MB)
        small_config = self.config["file_groups"]["small"]
        for i in range(small_config["count"]):
            size_mb = random.randint(1, small_config["max_size_mb"])
            file_path = self.local_temp_dir / f"small_{i}_{size_mb}MB.dat"
            self._create_random_file(file_path, size_mb * 1024 * 1024)
            file_list.append(file_path)
            
        # Create medium files (up to 5GB)
        medium_config = self.config["file_groups"]["medium"]
        for i in range(medium_config["count"]):
            size_mb = random.randint(100, medium_config["max_size_mb"])
            file_path = self.local_temp_dir / f"medium_{i}_{size_mb}MB.dat"
            self._create_random_file(file_path, size_mb * 1024 * 1024)
            file_list.append(file_path)
            
        # Create large files (up to 20GB)
        large_config = self.config["file_groups"]["large"]
        for i in range(large_config["count"]):
            size_mb = random.randint(1000, large_config["max_size_mb"])
            file_path = self.local_temp_dir / f"large_{i}_{size_mb}MB.dat"
            self._create_random_file(file_path, size_mb * 1024 * 1024)
            file_list.append(file_path)
            
        # Shuffle the file list for random processing
        random.shuffle(file_list)
        return file_list
        
    def _create_random_file(self, file_path: Path, size_bytes: int):
        """Create a random file of specified size"""
        with open(file_path, 'wb') as f:
            # Write in chunks to handle large files efficiently
            chunk_size = min(1024 * 1024, size_bytes)  # 1MB chunks or smaller
            remaining = size_bytes
            
            while remaining > 0:
                write_size = min(chunk_size, remaining)
                f.write(os.urandom(write_size))
                remaining -= write_size
                
    def upload_files(self, file_list: List[Path]) -> Dict[str, Any]:
        """Upload files to S3 using s5cmd in parallel with per-file progress"""
        cycle_start_time = time.time()
        uploaded_files = []
        failed_files = []
        
        # For cluster mode, we might want to use different endpoints
        s3_url = self.config["s3_urls"][0]  # Use first URL for now
        
        # Set environment variables for s5cmd
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
        
        # Track total bytes for successful uploads only
        total_bytes_uploaded = 0
        file_upload_info = []  # Track individual file upload info (size, time)
        
        # Create individual progress bars for each file
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            expand=True
        ) as progress:
            main_task = progress.add_task("[cyan]Uploading files...", total=len(file_list))
            file_tasks = {}
            
            # Process in batches based on parallel_threads setting
            batch_size = self.config["parallel_threads"]
            for i in range(0, len(file_list), batch_size):
                if not self.running:  # Check if user requested to stop
                    break
                    
                batch = file_list[i:i+batch_size]
                
                # Start parallel upload processes
                processes = []
                file_start_times = {}  # Track start times for each file
                for file_path in batch:
                    # Create s5cmd command for upload
                    s3_path = f"s3://{self.config['bucket_name']}/{file_path.name}"
                    
                    # Add individual file task
                    file_size = file_path.stat().st_size if file_path.exists() else 1024 * 1024  # Default 1MB if not exists
                    file_task = progress.add_task(f"[dim]{file_path.name}[/dim]", total=file_size)
                    file_tasks[str(file_path)] = file_task
                    
                    # Record start time for this file
                    file_start_times[str(file_path)] = time.time()
                    
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
                        processes.append((process, file_path, s3_path, file_start_times[str(file_path)], file_task))
                    except Exception as e:
                        failed_files.append((file_path, str(e)))
                        progress.update(main_task, advance=1)
                        if str(file_path) in file_tasks:
                            progress.remove_task(file_tasks[str(file_path)])
                
                # Poll processes with non-blocking approach
                completed_processes = []
                poll_count = 0
                max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
                
                while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                    poll_count += 1
                    for item in processes:
                        if item in completed_processes:
                            continue
                            
                        process, file_path, s3_path, start_time, file_task = item
                        
                        # Check if process has completed (non-blocking)
                        if process.poll() is not None:
                            # Process completed
                            stdout, stderr = process.communicate()
                            file_size = file_path.stat().st_size if file_path.exists() else 0
                            file_upload_time = time.time() - start_time
                            
                            if process.returncode == 0:
                                uploaded_files.append((file_path, s3_path))
                                # Track bytes for successful uploads
                                total_bytes_uploaded += file_size
                                # Track individual file upload info
                                file_upload_info.append((file_size, file_upload_time))
                                # Update file progress to complete
                                if file_path.exists():
                                    progress.update(file_task, completed=file_size)
                                self.console.print(f"[green]✓ {file_path.name} uploaded successfully[/green]")
                            else:
                                failed_files.append((file_path, stderr.decode() if stderr else "Unknown error"))
                                # Mark file as failed
                                progress.update(file_task, completed=0)
                                self.console.print(f"[red]✗ {file_path.name} failed to upload[/red]")
                            
                            completed_processes.append(item)
                            progress.update(main_task, advance=1)
                            # Remove individual file task
                            if str(file_path) in file_tasks:
                                progress.remove_task(file_tasks[str(file_path)])
                    
                    # Small delay to prevent busy waiting
                    time.sleep(0.1)
                
                # Check if we hit the polling limit
                if poll_count >= max_polls:
                    for item in processes:
                        if item not in completed_processes:
                            process, file_path, s3_path, start_time, file_task = item
                            process.kill()  # Kill the process
                            failed_files.append((file_path, "Timeout - process killed"))
                            self.console.print(f"[red]✗ {file_path.name} timed out and was killed[/red]")
                            progress.update(main_task, advance=1)
                            # Remove individual file task
                            if str(file_path) in file_tasks:
                                progress.remove_task(file_tasks[str(file_path)])
                    
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
        
    def download_files(self, s3_file_paths: List[str]) -> Dict[str, Any]:
        """Download files from S3 using s5cmd in parallel with per-file progress"""
        cycle_start_time = time.time()
        downloaded_files = []
        failed_files = []
        
        s3_url = self.config["s3_urls"][0]  # Use first URL for now
        
        # Set environment variables for s5cmd
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
        
        # Create individual progress bars for each file
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            expand=True
        ) as progress:
            main_task = progress.add_task("[blue]Reading files...", total=len(s3_file_paths))
            file_tasks = {}
            
            # Process in batches
            batch_size = self.config["parallel_threads"]
            for i in range(0, len(s3_file_paths), batch_size):
                if not self.running:  # Check if user requested to stop
                    break
                    
                batch = s3_file_paths[i:i+batch_size]
                
                # Start parallel read processes
                processes = []
                file_start_times = {}  # Track start times for each file
                for s3_path in batch:
                    file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                    
                    # Add individual file task (we'll simulate progress since we're reading to /dev/null)
                    file_task = progress.add_task(f"[dim]{file_name}[/dim]", total=100)
                    file_tasks[s3_path] = file_task
                    
                    # Record start time for this file
                    file_start_times[s3_path] = time.time()
                    
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
                        processes.append((process, s3_path, file_name, file_start_times[s3_path], file_task))
                    except Exception as e:
                        failed_files.append((s3_path, str(e)))
                        progress.update(main_task, advance=1)
                        if s3_path in file_tasks:
                            progress.remove_task(file_tasks[s3_path])
                
                # Poll processes with non-blocking approach
                completed_processes = []
                poll_count = 0
                max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
                
                while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                    poll_count += 1
                    for item in processes:
                        if item in completed_processes:
                            continue
                            
                        process, s3_path, file_name, start_time, file_task = item
                        
                        # Check if process has completed (non-blocking)
                        if process.poll() is not None:
                            # Process completed
                            stdout, stderr = process.communicate()
                            file_download_time = time.time() - start_time
                            
                            # For read operations, we consider it successful if the process completes
                            if process.returncode == 0:
                                downloaded_files.append(s3_path)
                                # Mark file as completed
                                progress.update(file_task, completed=100)
                                self.console.print(f"[green]✓ {file_name} read successfully[/green]")
                            else:
                                # Only consider it a failure if there's an actual error
                                error_output = stderr.decode().strip() if stderr else ""
                                if error_output and "no such file" not in error_output.lower():
                                    failed_files.append((s3_path, error_output))
                                    # Mark file as failed
                                    progress.update(file_task, completed=0)
                                    self.console.print(f"[red]✗ {file_name} failed to read[/red]")
                                else:
                                    # If it's just a "no such file" error or no error, still count as downloaded
                                    downloaded_files.append(s3_path)
                                    # Mark file as completed
                                    progress.update(file_task, completed=100)
                                    self.console.print(f"[green]✓ {file_name} read successfully[/green]")
                            
                            completed_processes.append(item)
                            progress.update(main_task, advance=1)
                            # Remove individual file task
                            if s3_path in file_tasks:
                                progress.remove_task(file_tasks[s3_path])
                        else:
                            # Update progress for ongoing downloads (simulate progress)
                            progress.update(file_task, advance=1)
                            # Reset to 0 if we've reached 100 to create a looping effect
                            if progress.tasks[file_task].completed >= 100:
                                progress.reset(file_task)
                    
                    # Small delay to prevent busy waiting
                    time.sleep(0.1)
                
                # Check if we hit the polling limit
                if poll_count >= max_polls:
                    for item in processes:
                        if item not in completed_processes:
                            process, s3_path, file_name, start_time, file_task = item
                            process.kill()  # Kill the process
                            failed_files.append((s3_path, "Timeout - process killed"))
                            self.console.print(f"[red]✗ {file_name} timed out and was killed[/red]")
                            progress.update(main_task, advance=1)
                            # Remove individual file task
                            if s3_path in file_tasks:
                                progress.remove_task(file_tasks[s3_path])
                    
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

    def delete_files(self, s3_file_paths: List[str]) -> Dict[str, Any]:
        """Delete files from S3 using s5cmd in parallel with per-file progress"""
        cycle_start_time = time.time()
        deleted_files = []
        failed_files = []
        
        s3_url = self.config["s3_urls"][0]  # Use first URL for now
        
        # Set environment variables for s5cmd
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = self.config["secret_key"]
        
        # Create individual progress bars for each file
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            expand=True
        ) as progress:
            main_task = progress.add_task("[red]Deleting files...", total=len(s3_file_paths))
            file_tasks = {}
            
            # Process in batches
            batch_size = self.config["parallel_threads"]
            for i in range(0, len(s3_file_paths), batch_size):
                if not self.running:  # Check if user requested to stop
                    break
                    
                batch = s3_file_paths[i:i+batch_size]
                
                # Start parallel delete processes
                processes = []
                file_start_times = {}  # Track start times for each file
                for s3_path in batch:
                    file_name = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                    
                    # Add individual file task
                    file_task = progress.add_task(f"[dim]{file_name}[/dim]", total=100)
                    file_tasks[s3_path] = file_task
                    
                    # Record start time for this file
                    file_start_times[s3_path] = time.time()
                    
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
                        processes.append((process, s3_path, file_name, file_start_times[s3_path], file_task))
                    except Exception as e:
                        failed_files.append((s3_path, str(e)))
                        progress.update(main_task, advance=1)
                        if s3_path in file_tasks:
                            progress.remove_task(file_tasks[s3_path])
                
                # Poll processes with non-blocking approach
                completed_processes = []
                poll_count = 0
                max_polls = 1200  # Limit polling to avoid infinite loop (1200 * 0.1s = 120 seconds)
                
                while len(completed_processes) < len(processes) and self.running and poll_count < max_polls:
                    poll_count += 1
                    for item in processes:
                        if item in completed_processes:
                            continue
                            
                        process, s3_path, file_name, start_time, file_task = item
                        
                        # Check if process has completed (non-blocking)
                        if process.poll() is not None:
                            # Process completed
                            stdout, stderr = process.communicate()
                            file_delete_time = time.time() - start_time
                            
                            if process.returncode == 0:
                                deleted_files.append(s3_path)
                                # Mark file as completed
                                progress.update(file_task, completed=100)
                                self.console.print(f"[green]✓ {file_name} deleted successfully[/green]")
                            else:
                                failed_files.append((s3_path, stderr.decode() if stderr else "Unknown error"))
                                # Mark file as failed
                                progress.update(file_task, completed=0)
                                self.console.print(f"[red]✗ {file_name} failed to delete[/red]")
                            
                            completed_processes.append(item)
                            progress.update(main_task, advance=1)
                            # Remove individual file task
                            if s3_path in file_tasks:
                                progress.remove_task(file_tasks[s3_path])
                        else:
                            # Update progress for ongoing deletions (simulate progress)
                            progress.update(file_task, advance=2)
                            # Reset to 0 if we've reached 100 to create a looping effect
                            if progress.tasks[file_task].completed >= 100:
                                progress.reset(file_task)
                    
                    # Small delay to prevent busy waiting
                    time.sleep(0.1)
                
                # Check if we hit the polling limit
                if poll_count >= max_polls:
                    for item in processes:
                        if item not in completed_processes:
                            process, s3_path, file_name, start_time, file_task = item
                            process.kill()  # Kill the process
                            failed_files.append((s3_path, "Timeout - process killed"))
                            self.console.print(f"[red]✗ {file_name} timed out and was killed[/red]")
                            progress.update(main_task, advance=1)
                            # Remove individual file task
                            if s3_path in file_tasks:
                                progress.remove_task(file_tasks[s3_path])
                    
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
        table.add_row("Files Downloaded", str(self.stats["files_downloaded"]))
        table.add_row("Files Deleted", str(self.stats["files_deleted"]))
        
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

    def run_test_cycle(self):
        """Run a single test cycle with randomized concurrent read/write operations"""
        self.console.print(Panel("[bold blue]Starting S3 Flood Test Cycle[/bold blue]"))
        
        # Create test files
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
        
        # Extract S3 paths for download operations
        s3_paths = [s3_path for _, s3_path in upload_result['uploaded']]
        
        # Shuffle S3 paths for random download order
        shuffled_s3_paths = s3_paths.copy()
        random.shuffle(shuffled_s3_paths)
        
        # Split files into two groups for concurrent operations
        mid_point = len(shuffled_s3_paths) // 2
        download_group1 = shuffled_s3_paths[:mid_point] if mid_point > 0 else shuffled_s3_paths
        download_group2 = shuffled_s3_paths[mid_point:] if mid_point > 0 else []
        
        # If we don't have enough files for both groups, use all files for both operations
        if not download_group2:
            download_group2 = download_group1.copy()
        
        self.console.print(f"[cyan]Starting randomized download operations...[/cyan]")
        self.console.print(f"[dim]  └─ Reading group 1: {len(download_group1)} files[/dim]")
        self.console.print(f"[dim]  └─ Reading group 2: {len(download_group2)} files[/dim]")
        
        # Perform sequential download operations to avoid Rich Live display conflicts
        self.console.print("[cyan]Reading files from S3 (group 1)...[/cyan]")
        download_result1 = self.download_files(download_group1)
        self.console.print(f"[green]✓ Read {len(download_result1['downloaded'])} files "
                          f"in {download_result1['time_elapsed']:.2f}s[/green]")
        
        self.console.print("[cyan]Reading files from S3 (group 2)...[/cyan]")
        download_result2 = self.download_files(download_group2)
        self.console.print(f"[green]✓ Read {len(download_result2['downloaded'])} files "
                          f"in {download_result2['time_elapsed']:.2f}s[/green]")
        
        # Combine download results
        total_downloaded = len(download_result1['downloaded']) + len(download_result2['downloaded'])
        total_download_time = download_result1['time_elapsed'] + download_result2['time_elapsed']
        
        # Delete all files
        self.console.print("[cyan]Deleting all files from S3...[/cyan]")
        delete_result = self.delete_files(s3_paths)
        self.console.print(f"[green]✓ Deleted {len(delete_result['deleted'])} files "
                          f"in {delete_result['time_elapsed']:.2f}s[/green]")
        
        # Update cycle count and stats
        self.stats["cycles_completed"] += 1
        self.stats["files_uploaded"] += len(upload_result['uploaded'])
        self.stats["files_downloaded"] += total_downloaded
        self.stats["total_upload_time"] += upload_result['time_elapsed']
        self.stats["total_download_time"] += total_download_time
        self.stats["files_deleted"] += len(delete_result['deleted'])
        self.stats["total_delete_time"] += delete_result['time_elapsed']
        
        # Clean up local files
        if self.local_temp_dir.exists():
            shutil.rmtree(self.local_temp_dir)
            self.console.print("[dim]✓ Cleaned up temporary files[/dim]")
            
        self.console.print(Panel("[bold green]Test Cycle Completed![/bold green]"))
        self.display_stats()
        
    def run_infinite_loop(self):
        """Run test cycles in an infinite loop"""
        self.console.print("[bold magenta]Starting infinite test loop...[/bold magenta]")
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
            
        self.config["access_key"] = questionary.text(
            "Access Key:",
            default=self.config.get("access_key", "minioadmin")
        ).ask()
        
        self.config["secret_key"] = questionary.text(
            "Secret Key:",
            default=self.config.get("secret_key", "minioadmin")
        ).ask()
        
        self.config["bucket_name"] = questionary.text(
            "Bucket Name:",
            default=self.config.get("bucket_name", "test-bucket")
        ).ask()
        
        self.config["cluster_mode"] = questionary.confirm(
            "Cluster Mode?",
            default=self.config.get("cluster_mode", False)
        ).ask()
        
        # Performance Configuration
        self.config["parallel_threads"] = int(questionary.text(
            "Parallel Threads:",
            default=str(self.config.get("parallel_threads", 5))
        ).ask())
        
        # File Configuration
        self.console.print("[cyan]File Group Configuration:[/cyan]")
        
        small_count = int(questionary.text(
            "Small files count (up to 100MB each):",
            default=str(self.config["file_groups"]["small"]["count"])
        ).ask())
        
        medium_count = int(questionary.text(
            "Medium files count (up to 5GB each):",
            default=str(self.config["file_groups"]["medium"]["count"])
        ).ask())
        
        large_count = int(questionary.text(
            "Large files count (up to 20GB each):",
            default=str(self.config["file_groups"]["large"]["count"])
        ).ask())
        
        self.config["file_groups"]["small"]["count"] = small_count
        self.config["file_groups"]["medium"]["count"] = medium_count
        self.config["file_groups"]["large"]["count"] = large_count
        
        # Test Configuration
        self.config["infinite_loop"] = questionary.confirm(
            "Run in infinite loop?",
            default=self.config.get("infinite_loop", True)
        ).ask()
        
        if self.config["infinite_loop"]:
            self.config["cycle_delay_seconds"] = int(questionary.text(
                "Delay between cycles (seconds):",
                default=str(self.config.get("cycle_delay_seconds", 15))
            ).ask())
            
        # Save configuration
        save_config = questionary.confirm(
            "Save configuration to config.yaml?",
            default=True
        ).ask()
        
        if save_config:
            self.save_config()
            
    def main_menu(self):
        """Display main menu and handle user choices"""
        while True:
            choice = questionary.select(
                "S3 Flood - Main Menu:",
                choices=[
                    "Run Test",
                    "Configure",
                    "View Statistics",
                    "Exit"
                ]
            ).ask()
            
            if choice == "Run Test":
                if not self.setup_s5cmd():
                    continue
                    
                if self.config.get("infinite_loop", True):
                    self.run_infinite_loop()
                else:
                    self.run_test_cycle()
                    
            elif choice == "Configure":
                self.interactive_config()
                
            elif choice == "View Statistics":
                self.display_stats()
                
            elif choice == "Exit":
                self.console.print("[bold green]Goodbye![/bold green]")
                break


def main():
    tester = S3FloodTester()
    tester.load_config()
    
    # Check if running in interactive mode or with command line args
    if len(sys.argv) > 1 and sys.argv[1] == "--config":
        tester.interactive_config()
    else:
        tester.main_menu()


if __name__ == "__main__":
    main()