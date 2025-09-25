#!/usr/bin/env python3
"""
Simple test script to verify per-file progress tracking
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester
from pathlib import Path

def main():
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
    # Create a few small test files
    print("Creating test files...")
    test_files = []
    for i in range(3):
        file_path = Path(f"test_file_{i}.txt")
        # Create a small file with some content
        with open(file_path, 'w') as f:
            f.write(f"This is test file {i}\n" * 100)  # 100 lines
        test_files.append(file_path)
        print(f"Created {file_path}")
    
    print(f"\nCreated {len(test_files)} test files")
    
    # Test upload with per-file progress
    print("\nTesting upload with per-file progress...")
    try:
        result = tester.upload_files(test_files)
        print(f"Uploaded {len(result['uploaded'])} files in {result['time_elapsed']:.2f}s")
        
        # Extract S3 paths for download
        s3_paths = [s3_path for _, s3_path in result['uploaded']]
        
        # Test download with per-file progress
        print("\nTesting download with per-file progress...")
        download_result = tester.download_files(s3_paths[:2])  # Download first 2 files
        print(f"Downloaded {len(download_result['downloaded'])} files in {download_result['time_elapsed']:.2f}s")
        
        # Test delete with per-file progress
        print("\nTesting delete with per-file progress...")
        delete_result = tester.delete_files(s3_paths)
        print(f"Deleted {len(delete_result['deleted'])} files in {delete_result['time_elapsed']:.2f}s")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Clean up local test files
    for file_path in test_files:
        if file_path.exists():
            file_path.unlink()
            print(f"Deleted local file {file_path}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    main()