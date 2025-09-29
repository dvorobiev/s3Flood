# Release Notes v1.6.0 - PowerShell Edition

## 🚀 Major New Feature: PowerShell Implementation

### ✅ Added PowerShell Version
- **New file**: [s3_flood_powershell.ps1](file:///Users/dvorobiev/s3Flood/s3_flood_powershell.ps1) - Full PowerShell implementation of S3 Flood
- **New file**: [install_powershell.ps1](file:///Users/dvorobiev/s3Flood/install_powershell.ps1) - Automated installation script for PowerShell version
- **New file**: [README_POWERSHELL.md](file:///Users/dvorobiev/s3Flood/README_POWERSHELL.md) - Complete documentation for PowerShell version

### 🔧 PowerShell Features
- **Native Windows Integration**: Uses PowerShell jobs for parallel operations
- **Built-in rclone Management**: Automatic download and installation of rclone
- **Color-coded Console Output**: Enhanced visual feedback with Windows console colors
- **Graceful Error Handling**: Proper exception handling and cleanup
- **Configurable Batch Processing**: Adjustable batch sizes for upload/download operations

### 📋 PowerShell Algorithm
1. **Generate Test Files**: Creates 100 mixed-size files (small, medium, large, huge)
2. **Batch Upload**: Uploads files in configurable batches using rclone
3. **Batch Download**: Downloads the same files in batches
4. **Delete All**: Removes all files from S3 bucket
5. **Repeat**: Waits 15 seconds and starts over

## 📁 File Structure Updates

### ✅ Core PowerShell Files
- [s3_flood_powershell.ps1](file:///Users/dvorobiev/s3Flood/s3_flood_powershell.ps1) - Main PowerShell script
- [install_powershell.ps1](file:///Users/dvorobiev/s3Flood/install_powershell.ps1) - Installation script
- [README_POWERSHELL.md](file:///Users/dvorobiev/s3Flood/README_POWERSHELL.md) - PowerShell documentation

### 📚 Documentation Updates
- Updated [README.md](file:///Users/dvorobiev/s3Flood/README.md) to include PowerShell version information
- Added reference to PowerShell files in main directory structure

## 🎯 User Benefits

### 🔧 For Windows Users
- **Alternative Implementation**: Choice between Python and PowerShell versions
- **No Python Required**: Pure PowerShell implementation
- **Automatic Dependency Management**: Install script handles rclone setup
- **Native Windows Experience**: Uses Windows-native tools and conventions

### ⚡ Performance Features
- **Parallel Jobs**: Uses PowerShell background jobs for concurrent operations
- **Configurable Batching**: Adjustable batch sizes for optimal performance
- **Real-time Feedback**: Color-coded progress updates
- **Automatic Cleanup**: Proper resource management and cleanup

## 📈 Version History

- **v1.5.3**: Critical fix for Windows batch file naming
- **v1.6.0**: Added complete PowerShell implementation with installation script

## 🚀 Getting Started with PowerShell Version

1. **Run Installation Script**:
   ```powershell
   .\install_powershell.ps1
   ```

2. **Configure rclone**:
   ```powershell
   .\tools\rclone.exe config
   ```

3. **Run S3 Flood**:
   ```powershell
   .\s3_flood_powershell.ps1
   ```

---

*This release expands S3 Flood's reach to Windows users with a native PowerShell implementation!* 🎉