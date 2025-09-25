#!/usr/bin/env python3
"""
Detailed connection test script with extended timeout and debugging
"""

import os
import sys
import subprocess
import time
from pathlib import Path

# Add the project directory to the path so we can import s5cmd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester

def main():
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
    print("=== Detailed S3 Connection Test ===")
    print(f"S3 URL: {tester.config['s3_urls'][0]}")
    print(f"Access Key: {tester.config['access_key'][:5]}...")
    print(f"Secret Key length: {len(tester.config['secret_key'])} characters")
    print(f"Bucket: {tester.config['bucket_name']}")
    print(f"Extended timeout: 120 seconds")
    print("")
    
    # Test s5cmd setup with extended timeout and debugging
    print("=== Testing s5cmd setup ===")
    
    # Check if s5cmd is available
    print("Checking if s5cmd is available...")
    try:
        result = subprocess.run(["s5cmd", "--help"], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✓ s5cmd is available")
        else:
            print("✗ s5cmd not found or not working")
            return
    except Exception as e:
        print(f"✗ Error checking s5cmd: {e}")
        return
    
    # Test S3 connection with provided credentials using environment variables
    s3_url = tester.config["s3_urls"][0]
    
    # Set environment variables for s5cmd
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = tester.config["access_key"]
    env["AWS_SECRET_ACCESS_KEY"] = tester.config["secret_key"]
    
    print(f"Testing S3 connection to {s3_url}...")
    print(f"Access Key: {tester.config['access_key'][:5]}...")
    print(f"Secret Key length: {len(tester.config['secret_key'])} characters")
    print(f"Bucket: {tester.config['bucket_name']}")
    
    # Create a simple test command to list buckets
    cmd = [
        "s5cmd", 
        "--endpoint-url", s3_url,
        "ls"
    ]
    
    print(f"Executing: {' '.join(cmd)}")
    print("This may take up to 120 seconds...")
    
    start_time = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)
        elapsed_time = time.time() - start_time
        
        print(f"Command completed in {elapsed_time:.2f} seconds")
        print(f"Return code: {result.returncode}")
        
        if result.returncode == 0:
            print("✓ S3 connection successful!")
            # Show the output
            if result.stdout:
                print("STDOUT:")
                print(result.stdout)
            else:
                print("No output received")
                
            if result.stderr:
                print("STDERR:")
                print(result.stderr)
        else:
            print("✗ S3 connection failed!")
            print(f"Return code: {result.returncode}")
            if result.stdout:
                print("STDOUT:")
                print(result.stdout)
            if result.stderr:
                print("STDERR:")
                print(result.stderr)
                
    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        print(f"✗ s5cmd test timed out after {elapsed_time:.2f} seconds")
        print("Possible causes:")
        print("  - Slow network connection to S3 endpoint")
        print("  - S3 endpoint is not responding")
        print("  - Firewall blocking connection")
        print("  - DNS resolution issues")
        print("  - Incorrect endpoint URL")
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"✗ Error during s5cmd test after {elapsed_time:.2f} seconds: {e}")

if __name__ == "__main__":
    main()