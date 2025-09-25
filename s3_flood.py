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
from typing import List, Dict, Any, Optional

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
        # Use test_files_directory from config or default to "./s3_temp_files"
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
            
    def create_test_files(self) -> List[Path]:
        """Create test files of different sizes with progress tracking"""
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
            "file_urls": file_urls  # Return URL mapping for cross-node testing
        }
        
    def download_files(self, s3_file_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
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

    def delete_files(self, s3_file_paths: List[str], upload_file_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
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
        download_result1 = self.download_files(download_group1, file_url_mapping)
        self.console.print(f"[green]✓ Read {len(download_result1['downloaded'])} files "
                          f"in {download_result1['time_elapsed']:.2f}s[/green]")
        
        self.console.print("[cyan]Reading files from S3 (group 2)...[/cyan]")
        download_result2 = self.download_files(download_group2, file_url_mapping)
        self.console.print(f"[green]✓ Read {len(download_result2['downloaded'])} files "
                          f"in {download_result2['time_elapsed']:.2f}s[/green]")
        
        # Combine download results
        total_downloaded = len(download_result1['downloaded']) + len(download_result2['downloaded'])
        total_download_time = download_result1['time_elapsed'] + download_result2['time_elapsed']
        
        # Delete all files
        self.console.print("[cyan]Deleting all files from S3...[/cyan]")
        delete_result = self.delete_files(s3_paths, file_url_mapping)
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