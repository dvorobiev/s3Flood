#!/usr/bin/env python3
"""
Simple test script to run infinite write mode with real S3 configuration
"""

import sys
import os

# Add parent directory to path to import s3_flood
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester

def main():
    # Initialize S3FloodTester with test config (smaller files)
    tester = S3FloodTester()
    tester.load_config('test_config.yaml')
    
    # Set algorithm to infinite write
    tester.algorithm = "infinite_write"
    
    # Set tool to s5cmd
    tester.tool = "s5cmd"
    
    # Setup s5cmd with provided credentials
    if tester.setup_s5cmd():
        print("✅ s5cmd setup successful")
        # Run infinite write cycle
        tester.run_infinite_write_cycle()
    else:
        print("❌ s5cmd setup failed")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())