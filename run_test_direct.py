#!/usr/bin/env python3
"""
Direct test script to run S3 flood test without interactive menu
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester

def main():
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
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
        # Run test cycle directly
        if tester.config.get("infinite_loop", True):
            tester.run_infinite_loop()
        else:
            tester.run_test_cycle()
    else:
        print("✗ s5cmd setup failed")

if __name__ == "__main__":
    main()