#!/usr/bin/env python3
"""
Test S3 connection with provided credentials
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def test_s3_connection():
    """Test S3 connection with provided credentials"""
    # Get credentials from environment variables or use placeholders
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_KEY")
    
    # Configuration
    config_file = "config.yaml"
    
    # Try to read from config file if it exists
    if Path(config_file).exists():
        try:
            import yaml
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                if 'access_key' in config:
                    access_key = config['access_key']
                if 'secret_key' in config:
                    secret_key = config['secret_key']
                if 's3_urls' in config and config['s3_urls']:
                    s3_url = config['s3_urls'][0]
                else:
                    s3_url = "http://localhost:9000"
                if 'bucket_name' in config:
                    bucket_name = config['bucket_name']
                else:
                    bucket_name = "test-bucket"
        except Exception as e:
            print(f"Error reading config file: {e}")
            s3_url = "http://localhost:9000"
            bucket_name = "test-bucket"
    else:
        s3_url = "http://localhost:9000"
        bucket_name = "test-bucket"
    
    print("=== S3 Connection Test ===")
    print(f"S3 URL: {s3_url}")
    print(f"Access Key: {access_key[:5]}...")  # Show only first 5 characters
    print(f"Secret Key length: {len(secret_key)} characters")
    print(f"Bucket: {bucket_name}")
    print(f"Timeout: 30 seconds")
    
    # Set environment variables for s5cmd
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    
    # Test s5cmd availability
    print("\nChecking if s5cmd is available...")
    try:
        result = subprocess.run(["s5cmd", "--help"], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✓ s5cmd is available")
        else:
            print("✗ s5cmd not found or not working")
            return False
    except FileNotFoundError:
        print("✗ s5cmd not found. Please install s5cmd first.")
        return False
    except subprocess.TimeoutExpired:
        print("✗ s5cmd test timed out")
        return False
    
    # Test S3 connection
    print(f"\nTesting S3 connection to {s3_url}...")
    cmd = [
        "s5cmd", 
        "--endpoint-url", s3_url,
        "ls"
    ]
    
    print("Executing: s5cmd --endpoint-url {} ls".format(s3_url))
    
    try:
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
        end_time = time.time()
        
        print(f"Command completed in {end_time - start_time:.2f} seconds")
        print(f"Return code: {result.returncode}")
        
        if result.returncode == 0:
            print("✓ S3 connection successful!")
            if result.stdout:
                print("Output:")
                print(result.stdout)
            return True
        else:
            print("✗ S3 connection failed!")
            if result.stderr:
                print("STDERR:")
                print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ s5cmd test timed out after 30 seconds. Check your network connection.")
        return False
    except Exception as e:
        print(f"✗ Error testing S3 connection: {e}")
        return False

if __name__ == "__main__":
    success = test_s3_connection()
    if success:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ s5cmd setup failed")
        sys.exit(1)