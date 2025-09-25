#!/usr/bin/env python3
"""
Test script to run a small S3 flood test
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester

def main():
    # Create tester instance
    tester = S3FloodTester()
    
    # Load test configuration
    tester.load_config("test_config.yaml")
    
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

if __name__ == "__main__":
    main()