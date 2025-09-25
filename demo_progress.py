#!/usr/bin/env python3
"""
Demo script to show per-file progress tracking and randomized concurrent operations
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
    
    # Run a single test cycle to demonstrate the features
    print("Running S3 Flood demo with per-file progress tracking...")
    print("This will show:")
    print("1. Per-file progress bars during upload operations")
    print("2. Randomized concurrent read operations")
    print("3. Per-file progress bars during delete operations")
    print("\n" + "="*50)
    
    try:
        tester.run_test_cycle()
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nError during demo: {e}")

if __name__ == "__main__":
    main()
