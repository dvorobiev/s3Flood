#!/usr/bin/env python3
"""
Real cluster test using your S3 configuration
"""

import sys
import os
import time
import signal
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from s3_flood import S3FloodTester

# Global flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    global running
    print("\n⚠️  Received interrupt signal. Stopping test...")
    running = False

def main():
    print("=" * 60)
    print("S3 FLOOD - REAL CLUSTER PERFORMANCE TEST")
    print("=" * 60)
    print("This test will run with your real S3 configuration:")
    print("  - 2 cluster nodes")
    print("  - 10 parallel threads")
    print("  - 16 large files (up to 16GB each)")
    print("  - Cluster mode enabled")
    print("=" * 60)
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create tester instance
    tester = S3FloodTester()
    
    # Load real configuration
    config_path = "real_s3_config.yaml"
    if os.path.exists(config_path):
        tester.load_config(config_path)
        print(f"✅ Loaded configuration from {config_path}")
    else:
        print(f"❌ Configuration file {config_path} not found")
        return
    
    # Display configuration
    print(f"\n🔧 Configuration:")
    print(f"   S3 URLs: {tester.config['s3_urls']}")
    print(f"   Cluster Mode: {tester.config['cluster_mode']}")
    print(f"   Parallel Threads: {tester.config['parallel_threads']}")
    print(f"   Bucket: {tester.config['bucket_name']}")
    print(f"   Large Files: {tester.config['file_groups']['large']['count']} "
          f"(up to {tester.config['file_groups']['large']['max_size_mb']}MB each)")
    print()
    
    # Set tool
    tester.tool = "s5cmd"  # or "rclone"
    print(f"✅ Using tool: {tester.tool}")
    print()
    
    # Override some settings for testing
    tester.config["infinite_loop"] = False  # Run only one cycle for testing
    tester.config["cycle_delay_seconds"] = 5
    
    print("🔧 Test Configuration:")
    print(f"   Infinite Loop: {tester.config['infinite_loop']}")
    print(f"   Cycle Delay: {tester.config['cycle_delay_seconds']}s")
    print()
    
    # Test connection first
    print("🔬 Testing S3 Connection...")
    connection_success = False
    
    try:
        if tester.tool == "s5cmd":
            connection_success = tester.setup_s5cmd()
        elif tester.tool == "rclone":
            connection_success = tester.setup_rclone()
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        connection_success = False
    
    if not connection_success:
        print("❌ S3 connection failed. Please check:")
        print("   1. Network connectivity to S3 endpoints")
        print("   2. Correctness of access_key and secret_key")
        print("   3. DNS resolution for endpoint hostnames")
        print("   4. Firewall settings")
        return
    
    print("✅ S3 connection successful!")
    print()
    
    # Run a single test cycle
    print("🚀 Starting Test Cycle...")
    print("-" * 25)
    
    try:
        # This will create files and run the test
        tester.run_test_cycle()
        
        print("\n✅ Test cycle completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
    
    # Show final statistics
    print("\n📊 FINAL STATISTICS")
    print("-" * 18)
    tester.display_stats()
    
    print("\n✅ Real cluster test completed!")

if __name__ == "__main__":
    main()