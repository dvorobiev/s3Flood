#!/usr/bin/env python3
"""
Clean demo script showing the simplified S3 Flood output
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
        "parallel_threads": 3,
        "file_groups": {
            "small": {"max_size_mb": 10, "count": 5},
            "medium": {"max_size_mb": 100, "count": 2},
            "large": {"max_size_mb": 500, "count": 0}
        },
        "infinite_loop": False,
        "cycle_delay_seconds": 5
    }
    
    # Test s5cmd setup
    if tester.setup_s5cmd():
        print("✓ s5cmd setup successful")
        # Run a single test cycle
        tester.run_test_cycle()
    else:
        print("✗ s5cmd setup failed")

if __name__ == "__main__":
    main()