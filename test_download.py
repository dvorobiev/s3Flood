#!/usr/bin/env python3
"""Простой тест для проверки чтения из S3"""
import subprocess
import json
import sys

def test_download(bucket, key, endpoint, access_key, secret_key):
    env = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_EC2_METADATA_DISABLED": "true"
    }
    
    cmd = [
        "aws", "s3api", "get-object",
        "--bucket", bucket,
        "--key", key,
        "/dev/null",
        "--endpoint-url", endpoint
    ]
    
    print(f"Testing download: bucket={bucket}, key={key}, endpoint={endpoint}")
    print(f"Command: {' '.join(cmd)}")
    
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    
    print(f"\nReturn code: {res.returncode}")
    print(f"Has returncode attr: {hasattr(res, 'returncode')}")
    print(f"Result type: {type(res)}")
    
    if res.stdout:
        print(f"\nSTDOUT ({len(res.stdout)} chars):")
        print(res.stdout[:500])
        try:
            data = json.loads(res.stdout)
            print(f"Parsed JSON: ContentLength={data.get('ContentLength', 'N/A')}")
        except:
            print("Could not parse as JSON")
    
    if res.stderr:
        print(f"\nSTDERR ({len(res.stderr)} chars):")
        print(res.stderr[:500])
    
    if res.returncode == 0:
        print("\n✓ SUCCESS: returncode == 0")
        return True
    else:
        print(f"\n✗ FAILED: returncode == {res.returncode}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Usage: test_download.py <bucket> <key> <endpoint> <access_key> <secret_key>")
        sys.exit(1)
    
    bucket = sys.argv[1]
    key = sys.argv[2]
    endpoint = sys.argv[3]
    access_key = sys.argv[4]
    secret_key = sys.argv[5]
    
    success = test_download(bucket, key, endpoint, access_key, secret_key)
    sys.exit(0 if success else 1)

