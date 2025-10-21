# S3 Flood Release Notes

## v1.6.3 - Rclone Support and Codebase Improvements

### 🚀 Major Feature: Rclone Support
- **Added rclone as alternative tool** for S3 operations alongside s5cmd
- **Tool selection menu** to choose between s5cmd and rclone at startup
- **Automatic rclone configuration** from application config file
- **Full feature parity** with s5cmd implementation:
  - Parallel upload operations with progress tracking
  - Parallel download operations with progress tracking
  - Parallel delete operations with progress tracking
  - Cross-platform support (Linux, Windows, macOS)

### 🛠️ Technical Improvements
- **Enhanced configuration management** with automatic rclone config updates
- **Improved error handling** for both s5cmd and rclone operations
- **Better code organization** with separate methods for each tool
- **Robust duplicate prevention** in rclone configuration files

### 🎯 User Benefits
- **Tool flexibility** - Choose the best tool for your environment
- **Enhanced compatibility** - Support for more S3-compatible services
- **Improved reliability** - Better error handling and recovery
- **Future-proof architecture** - Easy to extend with additional tools

---

## v1.6.2 - GitHub Badges

### 📛 Major Feature: GitHub Badges

#### ✅ Added Badges to Documentation
- **README.md** - Added badges for version, license, platform support, and Python version
- **README_POWERSHELL.md** - Added badges for version, license, platform support, and PowerShell version

#### 🎨 Badge Types Added
- **Version Badge**: ![Version](https://img.shields.io/badge/version-1.6.2-blue.svg)
- **License Badge**: ![License](https://img.shields.io/badge/license-MIT-green.svg)
- **Platform Badge**: ![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS%20%7C%20PowerShell-blue.svg)
- **Python Badge**: ![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)
- **PowerShell Badge**: ![PowerShell](https://img.shields.io/badge/powershell-5.1%2B-blue.svg)

#### 🎯 User Benefits
- **Quick Information**: At-a-glance version, license, platform compatibility, and system requirements
- **Visual Enhancement**: Professional appearance with consistent styling and color-coded information
- **Project Status**: Clear version tracking, license transparency, and technology stack identification

---

## v1.6.1 - Bilingual Documentation

### 🌍 Major Feature: Bilingual Documentation (English/Russian)

#### ✅ Updated Documentation Files
- **README.md** - Fully bilingual main documentation
- **README_POWERSHELL.md** - Fully bilingual PowerShell documentation
- **install_powershell.ps1** - Bilingual comments and messages
- **s3_flood_powershell.ps1** - Bilingual comments and messages
- **install.bat** - Bilingual comments and messages
- **run_windows.bat** - Bilingual comments and messages

#### 🎯 User Benefits
- **Language Accessibility**: Support for both English and Russian speakers
- **Improved Documentation**: Clear structure with language-separated sections
- **Enhanced Scripts**: User-friendly messages in both languages
- **Developer Support**: Localized comments for easier maintenance

---

## v1.6.0 - PowerShell Implementation

### ⚡ Major Feature: PowerShell Version

#### ✅ New PowerShell Implementation
- **s3_flood_powershell.ps1** - Complete PowerShell rewrite with full feature parity
- **install_powershell.ps1** - Automated installation script for PowerShell environment
- **README_POWERSHELL.md** - Comprehensive documentation for PowerShell version

#### 🎯 Key Features
- **Native PowerShell**: No Python dependencies required
- **Full Functionality**: Same features as Python version
- **Windows Optimization**: Better integration with Windows environment
- **Easy Installation**: One-click setup with install_powershell.ps1

---

## v1.5.2 - Enhanced Statistics and Performance

### 📊 Major Improvements

#### ✅ Enhanced Statistics Collection
- **Detailed Performance Metrics**: Upload/download speeds, operation times, success rates
- **Real-time Monitoring**: Live statistics during test execution
- **Comprehensive Reporting**: Detailed final reports with all metrics

#### ⚡ Performance Optimizations
- **Improved File Generation**: Faster creation of test files with better memory management
- **Optimized Parallel Processing**: Better thread management and resource utilization
- **Enhanced Progress Tracking**: More accurate progress indicators for large files

#### 🎯 User Benefits
- **Better Insights**: Detailed performance analysis for capacity planning
- **Faster Execution**: Improved performance with optimized resource usage
- **Reliable Results**: More accurate measurements with better error handling

---

## v1.5.0 - Cluster Mode and Infinite Loop

### 🏢 Major Features

#### ✅ Cluster Mode Support
- **Multi-Endpoint Testing**: Test multiple S3 endpoints simultaneously
- **Cross-Node Consistency**: Verify data consistency across cluster nodes
- **Load Distribution**: Distribute operations across multiple endpoints
- **Endpoint Failover**: Test failover scenarios with multiple URLs

#### 🔁 Infinite Loop Mode
- **Continuous Testing**: Run tests continuously until manually stopped
- **Configurable Delays**: Set delays between test cycles
- **Automatic Restart**: Resume testing after interruptions
- **Long-term Monitoring**: Monitor performance over extended periods

#### 🎯 User Benefits
- **Enterprise Scalability**: Test large-scale S3 clusters
- **Real-world Simulation**: Simulate production workloads
- **Reliability Testing**: Verify system stability under continuous load
- **Performance Monitoring**: Track performance degradation over time

---

## v1.0.0 - Initial Release

### 🚀 Core Features

#### ✅ Basic Functionality
- **Interactive TUI**: Easy setup and real-time monitoring
- **File Generation**: Create test files of various sizes
- **Parallel Operations**: Concurrent S3 operations using s5cmd
- **Progress Tracking**: Individual progress indicators for each operation
- **Statistics Collection**: Performance metrics and operation times
- **Cross-platform**: Support for Linux, Windows, and macOS

#### 🎯 Key Benefits
- **Easy Setup**: Simple installation and configuration
- **Flexible Testing**: Customizable file sizes and operation patterns
- **Real-time Feedback**: Live progress updates during testing
- **Comprehensive Reporting**: Detailed statistics and performance metrics