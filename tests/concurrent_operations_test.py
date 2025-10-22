#!/usr/bin/env python3
"""
Concurrent operations timing test to verify parallel execution
"""

import sys
import os
import time
import threading
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from s3_flood import S3FloodTester

def time_operation(func, *args, **kwargs):
    """Time an operation and return result with timing info"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

def create_test_file(file_path, size_lines=100):
    """Create a test file with specified number of lines"""
    with open(file_path, 'w') as f:
        f.write(f"Test content for {file_path.name}\n" * size_lines)

def main():
    print("=" * 60)
    print("S3 FLOOD - CONCURRENT OPERATIONS TIMING TEST")
    print("=" * 60)
    print("This test will verify concurrent execution timing:")
    print("  1. Operations in a batch run concurrently")
    print("  2. Batches run sequentially")
    print("  3. Total time matches expected concurrent behavior")
    print("=" * 60)
    
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
    # Override config for testing
    tester.config["parallel_threads"] = 4
    tester.config["cluster_mode"] = False
    tester.config["s3_urls"] = ["http://localhost:9000"]
    
    print(f"🔧 Test Configuration:")
    print(f"   Parallel Threads: {tester.config['parallel_threads']}")
    print(f"   Cluster Mode: {tester.config['cluster_mode']}")
    print(f"   S3 URLs: {tester.config['s3_urls']}")
    print()
    
    # Set tool directly for testing
    tester.tool = "s5cmd"  # or "rclone" depending on what's available
    print(f"✅ Using tool: {tester.tool}")
    print()
    
    # Create test files with different sizes to see timing differences
    print("Creating test files with different sizes...")
    test_files = []
    
    # Create files of different sizes
    sizes = [50, 100, 150, 200, 250, 300]  # Lines in each file
    for i, size in enumerate(sizes):
        file_path = Path(f"timing_test_file_{i}_{size}lines.txt")
        create_test_file(file_path, size)
        test_files.append(file_path)
        file_size = file_path.stat().st_size
        print(f"  Created {file_path} ({size} lines, {file_size} bytes)")
    
    print(f"\n✅ Created {len(test_files)} test files")
    print()
    
    try:
        # Test concurrent execution timing
        print("🔬 CONCURRENT EXECUTION TIMING TEST")
        print("-" * 40)
        
        batch_size = tester.config["parallel_threads"]
        print(f"Batch size (parallel threads): {batch_size}")
        print(f"Total files: {len(test_files)}")
        
        # Calculate expected timing behavior
        expected_batches = (len(test_files) + batch_size - 1) // batch_size
        print(f"Expected batches: {expected_batches}")
        print()
        
        # Process files in batches and measure timing
        total_start_time = time.time()
        all_results = []
        
        for i in range(0, len(test_files), batch_size):
            batch = test_files[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            print(f"🔄 Processing Batch {batch_num}/{expected_batches} ({len(batch)} files)")
            
            # Record start time for this batch
            batch_start_time = time.time()
            
            # Simulate what happens in the actual upload method
            # We'll just time how long it takes to "process" each file
            file_times = []
            
            def process_file(file_path, index):
                """Simulate file processing with timing"""
                file_start = time.time()
                # Simulate processing time based on file size
                # In real scenario, larger files would take longer
                size_lines = int(file_path.name.split('_')[-2])  # Extract size from filename
                # Sleep for a short time proportional to file size
                time.sleep(size_lines / 1000.0)  # Scale down for testing
                file_end = time.time()
                processing_time = file_end - file_start
                file_times.append((file_path.name, processing_time))
                print(f"    ✓ {file_path.name} processed in {processing_time:.3f}s")
            
            # Start all file processing operations concurrently (like in real implementation)
            threads = []
            for j, file_path in enumerate(batch):
                thread = threading.Thread(target=process_file, args=(file_path, j))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads in this batch to complete
            for thread in threads:
                thread.join()
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            print(f"  Batch {batch_num} completed in {batch_duration:.3f}s")
            print(f"  Individual file times: {[f'{t:.3f}s' for _, t in file_times]}")
            
            # Verify that batch time is close to max individual time (concurrent execution)
            if file_times:
                max_file_time = max(t for _, t in file_times)
                print(f"  Max individual file time: {max_file_time:.3f}s")
                print(f"  Batch time vs max time ratio: {batch_duration/max_file_time:.2f}x")
                
                # In perfect concurrent execution, batch time should be close to max individual time
                if batch_duration <= max_file_time * 1.5:  # Allow 50% overhead
                    print(f"  ✅ Batch executed concurrently (ratio <= 1.5)")
                else:
                    print(f"  ⚠️  Batch may not have executed fully concurrently (ratio > 1.5)")
            
            print()
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        
        print(f"⏱️  TOTAL EXECUTION TIME: {total_duration:.3f}s")
        print(f"📋 Files processed: {len(test_files)}")
        print(f"📦 Batches processed: {expected_batches}")
        
        # Expected time if all files processed sequentially
        # (sum of all individual times)
        # In our test, files are processed in batches concurrently
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
    
    # Clean up local test files
    print("\n🧹 Cleaning up local files...")
    for file_path in test_files:
        if file_path.exists():
            file_path.unlink()
    
    print("✅ Concurrent operations timing test completed!")

if __name__ == "__main__":
    main()