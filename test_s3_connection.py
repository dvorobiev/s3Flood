#!/usr/bin/env python3
"""
Simple S3 connection test script for debugging s5cmd connectivity
"""

import os
import sys
import subprocess
from rich.console import Console

def test_s3_connection():
    console = Console()
    
    # Get configuration from environment or use defaults
    s3_url = os.environ.get("S3_URL", "https://kazan.archive.systems:9080")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "MLAn0-sy5uv5qebve9dUQFEL")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "mwr3EBGY5SDO2eEft_r6m5KfPDSPFoRzv12JFQO_")
    bucket_name = os.environ.get("S3_BUCKET", "backup")
    
    console.print(f"[bold blue]Testing S3 Connection[/bold blue]")
    console.print(f"Endpoint: {s3_url}")
    console.print(f"Bucket: {bucket_name}")
    console.print(f"Access Key: {access_key[:5]}... (hidden)")
    console.print("")
    
    # Set environment variables for s5cmd
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    
    # Test 1: Check if s5cmd is available
    console.print("[cyan]Test 1: Checking if s5cmd is available...[/cyan]")
    try:
        result = subprocess.run(["s5cmd", "version"], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            console.print(f"[green]✓ s5cmd is available: {result.stdout.strip()}[/green]")
        else:
            console.print(f"[red]✗ s5cmd not found or not working[/red]")
            return False
    except FileNotFoundError:
        console.print(f"[red]✗ s5cmd not found in PATH[/red]")
        return False
    except Exception as e:
        console.print(f"[red]✗ Error checking s5cmd: {e}[/red]")
        return False
    
    # Test 2: List buckets
    console.print("[cyan]Test 2: Listing buckets...[/cyan]")
    cmd = ["s5cmd", "--endpoint-url", s3_url, "ls"]
    console.print(f"[dim]Command: {' '.join(cmd)}[/dim]")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
        if result.returncode == 0:
            console.print(f"[green]✓ Successfully listed buckets[/green]")
            if result.stdout.strip():
                console.print(f"[dim]Buckets:[/dim]")
                for line in result.stdout.strip().split('\n'):
                    console.print(f"  [dim]{line}[/dim]")
            else:
                console.print(f"[dim]No buckets found[/dim]")
        else:
            console.print(f"[red]✗ Failed to list buckets (return code: {result.returncode})[/red]")
            console.print(f"[red]STDERR: {result.stderr}[/red]")
            if result.stdout:
                console.print(f"[yellow]STDOUT: {result.stdout}[/yellow]")
            return False
    except subprocess.TimeoutExpired:
        console.print(f"[red]✗ s5cmd timed out after 30 seconds[/red]")
        return False
    except Exception as e:
        console.print(f"[red]✗ Error listing buckets: {e}[/red]")
        return False
    
    # Test 3: List objects in the specific bucket
    console.print("[cyan]Test 3: Listing objects in bucket...[/cyan]")
    s3_path = f"s3://{bucket_name}"
    cmd = ["s5cmd", "--endpoint-url", s3_url, "ls", s3_path]
    console.print(f"[dim]Command: {' '.join(cmd)}[/dim]")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
        if result.returncode == 0:
            console.print(f"[green]✓ Successfully listed objects in bucket[/green]")
            if result.stdout.strip():
                lines = result.stdout.strip().split('\n')
                console.print(f"[dim]Found {len(lines)} objects:[/dim]")
                # Show first 5 objects
                for line in lines[:5]:
                    console.print(f"  [dim]{line}[/dim]")
                if len(lines) > 5:
                    console.print(f"  [dim]... and {len(lines) - 5} more objects[/dim]")
            else:
                console.print(f"[dim]Bucket is empty[/dim]")
        else:
            console.print(f"[yellow]⚠ Failed to list objects in bucket (return code: {result.returncode})[/yellow]")
            console.print(f"[yellow]STDERR: {result.stderr}[/yellow]")
            if result.stdout:
                console.print(f"[dim]STDOUT: {result.stdout}[/dim]")
            # This is not necessarily a failure - the bucket might be empty
    except subprocess.TimeoutExpired:
        console.print(f"[red]✗ s5cmd timed out after 30 seconds[/red]")
        return False
    except Exception as e:
        console.print(f"[red]✗ Error listing objects: {e}[/red]")
        return False
    
    console.print("")
    console.print("[bold green]All tests completed successfully![/bold green]")
    console.print("[green]S3 connection is working properly.[/green]")
    return True

if __name__ == "__main__":
    test_s3_connection()