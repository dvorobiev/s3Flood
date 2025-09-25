#!/usr/bin/env python3
"""
Debug script to test S3 connection with detailed output
"""

import os
import sys
import subprocess
from pathlib import Path

# Add the project directory to the path so we can import s3_flood
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester

def main():
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
    print("=== S3 Connection Debug Info ===")
    print(f"S3 URL: {tester.config['s3_urls'][0]}")
    print(f"Access Key: {tester.config['access_key'][:5]}...")
    print(f"Secret Key length: {len(tester.config['secret_key'])} characters")
    print(f"Bucket: {tester.config['bucket_name']}")
    print(f"Parallel threads: {tester.config['parallel_threads']}")
    print("")
    
    # Test s5cmd setup with detailed output
    print("=== Testing s5cmd setup ===")
    result = tester.setup_s5cmd()
    print(f"Setup result: {result}")
    
    if result:
        print("\n=== Creating a small test file ===")
        # Create a small test file
        test_file = Path("test_connection.dat")
        with open(test_file, 'wb') as f:
            f.write(b"Hello, S3!" * 100)  # 1KB test file
        print(f"Created test file: {test_file} ({test_file.stat().st_size} bytes)")
        
        print("\n=== Testing file upload ===")
        # Test uploading a single file
        s3_url = tester.config["s3_urls"][0]
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = tester.config["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = tester.config["secret_key"]
        
        s3_path = f"s3://{tester.config['bucket_name']}/{test_file.name}"
        cmd = ["s5cmd", "--endpoint-url", s3_url, "cp", str(test_file), s3_path]
        print(f"Executing: {' '.join(cmd)}")
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            stdout, stderr = process.communicate(timeout=30)
            
            print(f"Return code: {process.returncode}")
            if stdout:
                print(f"STDOUT: {stdout.decode()}")
            if stderr:
                print(f"STDERR: {stderr.decode()}")
                
            if process.returncode == 0:
                print("✓ File upload successful!")
                
                print("\n=== Testing file listing ===")
                # Test listing files
                cmd = ["s5cmd", "--endpoint-url", s3_url, "ls", f"s3://{tester.config['bucket_name']}/"]
                print(f"Executing: {' '.join(cmd)}")
                
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
                stdout, stderr = process.communicate(timeout=30)
                
                print(f"Return code: {process.returncode}")
                if stdout:
                    print(f"STDOUT: {stdout.decode()}")
                if stderr:
                    print(f"STDERR: {stderr.decode()}")
                    
                print("\n=== Testing file deletion ===")
                # Test deleting the file
                cmd = ["s5cmd", "--endpoint-url", s3_url, "rm", s3_path]
                print(f"Executing: {' '.join(cmd)}")
                
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
                stdout, stderr = process.communicate(timeout=30)
                
                print(f"Return code: {process.returncode}")
                if stdout:
                    print(f"STDOUT: {stdout.decode()}")
                if stderr:
                    print(f"STDERR: {stderr.decode()}")
                    
                if process.returncode == 0:
                    print("✓ File deletion successful!")
                else:
                    print("✗ File deletion failed!")
            else:
                print("✗ File upload failed!")
                
        except subprocess.TimeoutExpired:
            print("✗ Operation timed out!")
        except Exception as e:
            print(f"✗ Error: {e}")
        finally:
            # Clean up test file
            if test_file.exists():
                test_file.unlink()
                print(f"Cleaned up local test file: {test_file}")
    else:
        print("✗ s5cmd setup failed!")

if __name__ == "__main__":
    main()