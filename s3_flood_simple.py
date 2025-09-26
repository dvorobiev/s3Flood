#!/usr/bin/env python3
\"\"\"
S3 Flood Simple - Windows compatible version without rich/questionary
\"\"\"

import os
import sys
import yaml
import subprocess
import platform
from pathlib import Path

def get_version() -> str:
    \"\"\"Get version from VERSION file\"\"\"
    try:
        with open(Path(__file__).parent / \"VERSION\", \"r\") as f:
            return f.read().strip()
    except FileNotFoundError:
        return \"development\"

class SimpleS3FloodTester:
    def __init__(self):
        self.config = {}
        
    def print_header(self):
        print(\"=\"*50)
        print(f\"S3 Flood v{get_version()} - Simple Windows Version\")
        print(\"=\"*50)
        print()
        
    def load_config(self, config_path=\"config.yaml\"):
        \"\"\"Load configuration from YAML file\"\"\"
        default_config = {
            \"s3_urls\": [\"http://localhost:9000\"],
            \"access_key\": \"minioadmin\",
            \"secret_key\": \"minioadmin\",
            \"bucket_name\": \"test-bucket\",
            \"cluster_mode\": False,
            \"parallel_threads\": 5,
            \"file_groups\": {
                \"small\": {\"max_size_mb\": 100, \"count\": 10},
                \"medium\": {\"max_size_mb\": 1024, \"count\": 5},
                \"large\": {\"max_size_mb\": 5120, \"count\": 2}
            },
            \"infinite_loop\": True,
            \"cycle_delay_seconds\": 15,
            \"test_files_directory\": \"./s3_temp_files\"
        }
        
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
        except FileNotFoundError:
            print(f\"Config file {config_path} not found. Creating default config.\")
            self.config = default_config
            self.save_config(config_path)
            
    def save_config(self, config_path=\"config.yaml\"):
        \"\"\"Save configuration to YAML file\"\"\"
        try:
            with open(config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            print(f\"Configuration saved to {config_path}\")
        except Exception as e:
            print(f\"Error saving config: {e}\")
            
    def test_s5cmd(self):
        \"\"\"Test s5cmd connectivity\"\"\"
        try:
            env = os.environ.copy()
            env[\"AWS_ACCESS_KEY_ID\"] = self.config[\"access_key\"]
            env[\"AWS_SECRET_ACCESS_KEY\"] = self.config[\"secret_key\"]
            
            cmd = [\"s5cmd\", \"--endpoint-url\", self.config[\"s3_urls\"][0], \"ls\"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
            
            if result.returncode == 0:
                print(\"[SUCCESS] S3 connection test passed\")
                return True
            else:
                print(f\"[ERROR] S3 connection failed: {result.stderr}\")
                return False
        except Exception as e:
            print(f\"[ERROR] s5cmd test failed: {e}\")
            return False
            
    def interactive_config(self):
        \"\"\"Simple configuration without rich\"\"\"
        print(\"\nS3 Flood Configuration\")
        print(\"-\" * 30)
        
        # S3 URLs
        current_urls = \",\".join(self.config.get(\"s3_urls\", [\"http://localhost:9000\"]))
        urls_input = input(f\"S3 Endpoint URLs [{current_urls}]: \").strip()
        if urls_input:
            self.config[\"s3_urls\"] = [url.strip() for url in urls_input.split(\",\")]
            
        # Access credentials
        access_key = input(f\"Access Key [{self.config.get('access_key', 'minioadmin')}]: \").strip()
        if access_key:
            self.config[\"access_key\"] = access_key
            
        secret_key = input(f\"Secret Key [{self.config.get('secret_key', 'minioadmin')}]: \").strip()
        if secret_key:
            self.config[\"secret_key\"] = secret_key
            
        # Bucket name
        bucket = input(f\"Bucket Name [{self.config.get('bucket_name', 'test-bucket')}]: \").strip()
        if bucket:
            self.config[\"bucket_name\"] = bucket
            
        # Cluster mode
        cluster_choice = input(f\"Cluster Mode? (y/n) [{self.config.get('cluster_mode', False)}]: \").strip().lower()
        if cluster_choice in ['y', 'yes']:
            self.config[\"cluster_mode\"] = True
        elif cluster_choice in ['n', 'no']:
            self.config[\"cluster_mode\"] = False
            
        # Save configuration
        save_choice = input(\"Save configuration? (y/n) [y]: \").strip().lower()
        if save_choice != 'n':
            self.save_config()
            
    def main_menu(self):
        \"\"\"Simple main menu\"\"\"
        while True:
            self.print_header()
            print(\"Main Menu:\")
            print(\"1. Test S3 Connection\")
            print(\"2. Configure\")
            print(\"3. Run Quick Test\")
            print(\"4. Exit\")
            print()
            
            try:
                choice = input(\"Select option (1-4): \").strip()
                
                if choice == \"1\":
                    print(\"\nTesting S3 connection...\")
                    self.test_s5cmd()
                    input(\"\nPress Enter to continue...\")
                    
                elif choice == \"2\":
                    self.interactive_config()
                    
                elif choice == \"3\":
                    print(\"\n[INFO] Quick test functionality would be implemented here\")
                    print(\"[INFO] This version focuses on configuration and connectivity testing\")
                    input(\"\nPress Enter to continue...\")
                    
                elif choice == \"4\":
                    print(\"\nGoodbye!\")
                    break
                    
                else:
                    print(\"\n[ERROR] Invalid choice. Please enter 1-4.\")
                    input(\"Press Enter to continue...\")
                    
            except (KeyboardInterrupt, EOFError):
                print(\"\n\nExiting...\")
                break
                
def main():
    tester = SimpleS3FloodTester()
    tester.load_config()
    tester.main_menu()

if __name__ == \"__main__\":
    main()
"