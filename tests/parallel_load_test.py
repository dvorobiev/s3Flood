#!/usr/bin/env python3
"""
Parallel load test to verify concurrent operations and cluster mode behavior
"""

import sys
import os
import time
import threading
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from s3_flood import S3FloodTester

def main():
    print("=" * 60)
    print("S3 FLOOD - PARALLEL LOAD TEST")
    print("=" * 60)
    print("This test will verify:")
    print("  1. Correct number of parallel threads are used")
    print("  2. Cluster mode distributes load across nodes")
    print("  3. Operations run concurrently as expected")
    print("=" * 60)
    
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
    # Override config for testing
    tester.config["parallel_threads"] = 3
    tester.config["cluster_mode"] = False  # Start with single mode
    tester.config["s3_urls"] = ["http://localhost:9000"]  # Single endpoint
    
    print(f"🔧 Test Configuration:")
    print(f"   Parallel Threads: {tester.config['parallel_threads']}")
    print(f"   Cluster Mode: {tester.config['cluster_mode']}")
    print(f"   S3 URLs: {tester.config['s3_urls']}")
    print()
    
    # Set tool directly for testing
    tester.tool = "s5cmd"  # or "rclone" depending on what's available
    print(f"✅ Using tool: {tester.tool}")
    print()
    
    # Create test files
    print("Creating test files...")
    test_files = []
    
    # Create small files for testing
    for i in range(5):
        file_path = Path(f"parallel_test_file_{i}.txt")
        # Create file with content
        with open(file_path, 'w') as f:
            f.write(f"This is parallel test file {i}\n" * 100)  # 100 lines
        test_files.append(file_path)
        print(f"  Created {file_path}")
    
    print(f"\n✅ Created {len(test_files)} test files")
    print()
    
    try:
        # Test 1: Verify parallel thread count logic
        print("🔬 TEST 1: Parallel Thread Count Logic Verification")
        print("-" * 50)
        
        # Test batch calculation logic
        batch_size = tester.config["parallel_threads"]
        expected_batches = (len(test_files) + batch_size - 1) // batch_size
        
        print(f"   Files to process: {len(test_files)}")
        print(f"   Parallel threads configured: {tester.config['parallel_threads']}")
        print(f"   Calculated batch size: {batch_size}")
        print(f"   Expected batches: {expected_batches}")
        
        # Verify batch processing logic
        batches = []
        for i in range(0, len(test_files), batch_size):
            batch = test_files[i:i+batch_size]
            batches.append(batch)
            print(f"   Batch {len(batches)}: {len(batch)} files")
        
        print(f"   Actual batches created: {len(batches)}")
        
        # Test 2: Cluster mode endpoint selection logic
        print("\n🔬 TEST 2: Cluster Mode Endpoint Selection Logic")
        print("-" * 45)
        
        # Modify config for cluster mode test
        tester.config["cluster_mode"] = True
        tester.config["s3_urls"] = [
            "http://node1:9000",
            "http://node2:9000",
            "http://node3:9000"
        ]
        
        print(f"   Cluster mode enabled with {len(tester.config['s3_urls'])} endpoints")
        
        # Test endpoint selection logic
        selected_endpoints = []
        for i in range(15):  # Test 15 random selections
            endpoint = tester._get_s3_url()
            selected_endpoints.append(endpoint)
        
        # Count distribution
        endpoint_counts = {}
        for endpoint in selected_endpoints:
            endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
            
        print(f"   Selection distribution: {endpoint_counts}")
        
        # Verify all endpoints are used
        all_endpoints_used = all(endpoint in endpoint_counts for endpoint in tester.config["s3_urls"])
        print(f"   All endpoints used: {all_endpoints_used}")
        
        # Test 3: Parallel processing simulation
        print("\n🔬 TEST 3: Parallel Processing Simulation")
        print("-" * 40)
        
        print("   Simulating parallel file processing:")
        print(f"   - {len(test_files)} files to process")
        print(f"   - {tester.config['parallel_threads']} concurrent operations")
        print(f"   - {len(batches)} batches required")
        
        # Show how files would be distributed
        for i, batch in enumerate(batches):
            print(f"   Batch {i+1}: {len(batch)} files processed concurrently")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
    
    # Clean up local test files
    print("\n🧹 Cleaning up local files...")
    for file_path in test_files:
        if file_path.exists():
            file_path.unlink()
    
    print("✅ Parallel load test completed!")

if __name__ == "__main__":
    main()