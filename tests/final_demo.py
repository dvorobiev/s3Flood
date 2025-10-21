#!/usr/bin/env python3
"""
Final demo script to showcase per-file progress tracking
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_flood import S3FloodTester
from pathlib import Path

def main():
    print("=" * 60)
    print("S3 FLOOD - PER-FILE PROGRESS TRACKING DEMO")
    print("=" * 60)
    print("This demo will show per-file progress bars for:")
    print("  1. Upload operations")
    print("  2. Download operations") 
    print("  3. Delete operations")
    print("=" * 60)
    
    # Create tester instance
    tester = S3FloodTester()
    
    # Load configuration
    tester.load_config()
    
    # Check S3 connection
    if not tester.setup_s5cmd():
        print("❌ Failed to setup s5cmd. Please check your configuration.")
        return
    
    print("✅ S3 connection established successfully!")
    print()
    
    # Create a few test files of different sizes
    print("Creating test files...")
    test_files = []
    
    # Create small files
    for i in range(3):
        file_path = Path(f"demo_file_{i}.txt")
        # Create file with different sizes
        size_multiplier = [10, 50, 100][i]  # 10, 50, 100 lines
        with open(file_path, 'w') as f:
            f.write(f"This is demo file {i}\n" * size_multiplier)
        test_files.append(file_path)
        print(f"  Created {file_path} ({size_multiplier} lines)")
    
    print(f"\n✅ Created {len(test_files)} test files")
    print()
    
    try:
        # Demonstrate upload with per-file progress
        print("📂 UPLOADING FILES TO S3")
        print("-" * 30)
        upload_result = tester.upload_files(test_files)
        print(f"✅ Uploaded {len(upload_result['uploaded'])} files in {upload_result['time_elapsed']:.2f}s")
        print()
        
        # Extract S3 paths for download
        s3_paths = [s3_path for _, s3_path in upload_result['uploaded']]
        
        # Demonstrate download with per-file progress
        print("📥 DOWNLOADING FILES FROM S3")
        print("-" * 30)
        download_result = tester.download_files(s3_paths)
        print(f"✅ Downloaded {len(download_result['downloaded'])} files in {download_result['time_elapsed']:.2f}s")
        print()
        
        # Demonstrate delete with per-file progress
        print("🗑️  DELETING FILES FROM S3")
        print("-" * 30)
        delete_result = tester.delete_files(s3_paths)
        print(f"✅ Deleted {len(delete_result['deleted'])} files in {delete_result['time_elapsed']:.2f}s")
        print()
        
        # Show final statistics
        print("📊 FINAL STATISTICS")
        print("-" * 20)
        tester.display_stats()
        
    except Exception as e:
        print(f"❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()
    
    # Clean up local test files
    print("\n🧹 Cleaning up local files...")
    for file_path in test_files:
        if file_path.exists():
            file_path.unlink()
    
    print("✅ Demo completed successfully!")

if __name__ == "__main__":
    main()