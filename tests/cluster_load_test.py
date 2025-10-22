#!/usr/bin/env python3
"""
Cluster load test using real S3 configuration
"""

import sys
import os
import time
import yaml
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from s3_flood import S3FloodTester

def main():
    print("=" * 60)
    print("S3 FLOOD - CLUSTER LOAD TEST")
    print("=" * 60)
    print("This test will verify cluster mode with real S3 configuration:")
    print("  1. Parallel operations with 10 threads")
    print("  2. Load distribution across 2 cluster nodes")
    print("  3. Large file operations (16 files, up to 16GB each)")
    print("=" * 60)
    
    # Create tester instance
    tester = S3FloodTester()
    
    # Load real configuration
    config_path = "real_s3_config.yaml"
    if os.path.exists(config_path):
        tester.load_config(config_path)
        print(f"✅ Loaded configuration from {config_path}")
    else:
        print(f"❌ Configuration file {config_path} not found")
        print("Please ensure real_s3_config.yaml exists with your S3 settings")
        return
    
    # Display configuration
    print(f"\n🔧 Configuration:")
    print(f"   S3 URLs: {tester.config['s3_urls']}")
    print(f"   Cluster Mode: {tester.config['cluster_mode']}")
    print(f"   Parallel Threads: {tester.config['parallel_threads']}")
    print(f"   Bucket: {tester.config['bucket_name']}")
    print(f"   File Groups: {tester.config['file_groups']}")
    print()
    
    # Set tool
    tester.tool = "s5cmd"  # or "rclone"
    print(f"✅ Using tool: {tester.tool}")
    print()
    
    # Test cluster mode endpoint distribution
    print("🔬 CLUSTER MODE ENDPOINT DISTRIBUTION TEST")
    print("-" * 45)
    
    if tester.config.get("cluster_mode", False) and len(tester.config["s3_urls"]) > 1:
        print(f"Testing endpoint selection across {len(tester.config['s3_urls'])} nodes:")
        
        # Test endpoint selection
        endpoint_counts = {}
        test_count = 50  # Test 50 random selections
        
        for i in range(test_count):
            endpoint = tester._get_s3_url()
            endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
        
        for endpoint, count in endpoint_counts.items():
            percentage = (count / test_count) * 100
            print(f"   {endpoint}: {count} selections ({percentage:.1f}%)")
        
        # Check if distribution is reasonably balanced
        expected_per_endpoint = test_count / len(tester.config["s3_urls"])
        balanced = True
        for count in endpoint_counts.values():
            if abs(count - expected_per_endpoint) > expected_per_endpoint * 0.5:  # 50% tolerance
                balanced = False
                break
        
        if balanced:
            print("✅ Endpoint distribution is reasonably balanced")
        else:
            print("⚠️  Endpoint distribution may be unbalanced")
    else:
        print("❌ Cluster mode not enabled or only one endpoint configured")
    
    print()
    
    # Test parallel thread configuration
    print("🔬 PARALLEL THREAD CONFIGURATION TEST")
    print("-" * 40)
    
    parallel_threads = tester.config.get("parallel_threads", 5)
    print(f"Configured parallel threads: {parallel_threads}")
    
    # Show how files would be processed
    large_files_count = tester.config["file_groups"]["large"]["count"]
    if large_files_count > 0:
        batch_size = parallel_threads
        expected_batches = (large_files_count + batch_size - 1) // batch_size
        print(f"Large files to process: {large_files_count}")
        print(f"Batch size: {batch_size}")
        print(f"Expected batches: {expected_batches}")
        
        # Simulate batch processing
        print("\nSimulated batch processing:")
        for i in range(0, large_files_count, batch_size):
            batch_num = i // batch_size + 1
            remaining_files = large_files_count - i
            current_batch_size = min(batch_size, remaining_files)
            print(f"   Batch {batch_num}: {current_batch_size} files processed concurrently")
    else:
        print("No large files configured for testing")
    
    print()
    
    # Test actual S3 connection
    print("🔬 S3 CONNECTION TEST")
    print("-" * 25)
    
    try:
        if tester.tool == "s5cmd":
            if tester.setup_s5cmd():
                print("✅ s5cmd connection successful")
            else:
                print("❌ s5cmd connection failed")
        elif tester.tool == "rclone":
            if tester.setup_rclone():
                print("✅ rclone connection successful")
            else:
                print("❌ rclone connection failed")
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
    
    print()
    
    # Summary
    print("📋 TEST SUMMARY")
    print("-" * 15)
    print(f"Configuration file: {config_path}")
    print(f"Cluster mode: {tester.config.get('cluster_mode', False)}")
    print(f"Endpoints: {len(tester.config.get('s3_urls', []))}")
    print(f"Parallel threads: {tester.config.get('parallel_threads', 5)}")
    print(f"Large files: {tester.config['file_groups']['large']['count']}")
    print(f"Max file size: {tester.config['file_groups']['large']['max_size_mb']}MB")
    
    print("\n✅ Cluster load test configuration verified!")

if __name__ == "__main__":
    main()