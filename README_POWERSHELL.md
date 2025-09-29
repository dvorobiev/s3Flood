# S3 Flood - PowerShell Edition

A PowerShell implementation of the S3 Flood tool for testing S3-compatible storage performance with parallel upload/download operations.

## 📋 Features

- **Parallel Operations**: Upload and download files concurrently using PowerShell jobs
- **Batch Processing**: Process files in configurable batches (default: 10 files per batch)
- **Mixed File Sizes**: Generates test files of various sizes (small, medium, large, huge)
- **Automatic Cleanup**: Cleans up temporary files and S3 objects after each cycle
- **Real-time Logging**: Color-coded console output with timestamps
- **Graceful Shutdown**: Properly handles Ctrl+C interruption

## 📁 File Structure

```
s3Flood/
├── s3_flood_powershell.ps1     # Main PowerShell script
├── README_POWERSHELL.md        # This file
└── rclone                      # Required rclone binary (must be in same directory)
```

## ⚙️ Requirements

1. **Windows PowerShell 5.1** or **PowerShell 7+**
2. **rclone** binary in the same directory as the script
3. **S3-compatible storage** with configured rclone remote

## 🚀 Quick Start

1. **Download rclone**:
   - Download rclone for Windows from [https://rclone.org/downloads/](https://rclone.org/downloads/)
   - Extract `rclone.exe` to the same directory as the script

2. **Configure rclone**:
   ```bash
   ./rclone config
   ```
   Create a remote named `demo` (or change `$rcloneRemote` in the script)

3. **Run the script**:
   ```powershell
   .\s3_flood_powershell.ps1
   ```

## 🛠️ Configuration

Edit these variables at the top of [s3_flood_powershell.ps1](file:///Users/dvorobiev/s3Flood/s3_flood_powershell.ps1):

```powershell
# Configuration
$rcloneRemote = "demo"        # Your rclone remote name
$bucketName = "backup"        # S3 bucket name
$localTempDir = ".\S3_TEMP_FILES"  # Local temp directory
$batchSize = 10               # Files per batch
```

## 📊 Test File Generation

The script automatically generates 100 test files:

- **30 Small files**: 1MB - 100MB
- **30 Medium files**: 101MB - 1024MB
- **30 Large files**: 1GB - 10GB
- **10 Huge files**: 11GB - 100GB

## 🔁 Algorithm

1. **Generate Test Files**: Create 100 random-sized files in temp directory
2. **Batch Upload**: Upload files in batches of `$batchSize`
3. **Batch Download**: Download the same files in batches
4. **Delete All**: Remove all files from S3 bucket
5. **Repeat**: Wait 15 seconds and start over

## 🎨 Console Colors

- **Gray**: General information
- **Green**: Success messages
- **Yellow**: Warnings and deletion operations
- **Red**: Errors
- **Cyan**: Batch processing information
- **Magenta**: Cycle completion

## ⚠️ Important Notes

- The script will **delete all files** from the specified bucket during testing
- Make sure to use a **dedicated test bucket**
- Large files (1GB+) may take significant time to generate
- The script runs indefinitely until stopped with Ctrl+C
- Requires administrator privileges to create large files with `fsutil`

## 🛑 Stopping the Script

Press `Ctrl+C` to gracefully stop the script. It will:
- Cancel all running jobs
- Delete temporary files
- Clean up S3 objects
- Exit cleanly

## 📈 Monitoring

The script provides real-time feedback:
- Batch progress
- File counts
- Operation status
- Timing information

## 🔧 Troubleshooting

### "fsutil is not recognized"
Run PowerShell as Administrator

### "rclone not found"
Ensure `rclone.exe` is in the same directory as the script

### "Access denied"
Check S3 bucket permissions and rclone configuration

### "Out of disk space"
The script requires significant disk space for temporary files (up to several hundred GB)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.