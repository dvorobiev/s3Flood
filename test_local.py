#!/usr/bin/env python3
"""
Test script to run a small S3 flood test with local MinIO
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester

def main():
    # Create tester instance
    tester = S3FloodTester()
    
    # Load test configuration for local MinIO
    tester.config = {
        "s3_urls": ["http://localhost:9000"],
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket_name": "test-bucket",
        "cluster_mode": False,
        "parallel_threads": 2,
        "file_groups": {
            "small": {"max_size_mb": 10, "count": 3},
            "medium": {"max_size_mb": 150, "count": 1},  # Must be >= 100
            "large": {"max_size_mb": 1500, "count": 0}   # Must be >= 1000
        },
        "infinite_loop": False,
        "cycle_delay_seconds": 5
    }
    
    # Show configuration
    print("Test Configuration:")
    print(f"  Endpoint: {tester.config['s3_urls'][0]}")
    print(f"  Bucket: {tester.config['bucket_name']}")
    print(f"  Parallel threads: {tester.config['parallel_threads']}")
    print(f"  Small files: {tester.config['file_groups']['small']['count']} (up to {tester.config['file_groups']['small']['max_size_mb']}MB)")
    print(f"  Infinite loop: {tester.config['infinite_loop']}")
    print("")
    
    # Test s5cmd setup
    if tester.setup_s5cmd():
        print("✓ s5cmd setup successful")
        # Run a single test cycle
        tester.run_test_cycle()
    else:
        print("✗ s5cmd setup failed")
        print("Please make sure MinIO is running on localhost:9000")
        print("You can start MinIO with: docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ':9001'")

if __name__ == "__main__":
    main()