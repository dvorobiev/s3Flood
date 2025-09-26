# S3 Flood Windows Installation Guide# S3 Flood - Windows Installation Guide

## 🚀 Quick Start (Recommended)

### Method 1: Automatic Installation
1. Download the `windows-support` branch
2. Run `install.bat`
3. Use `run_windows.bat` for best compatibility

### Method 2: Manual Installation
1. Install Python 3.7+ from [python.org](https://www.python.org/downloads/)
2. During installation, **check "Add Python to PATH"**
3. Install dependencies: `pip install pyyaml`
4. Run: `python s3_flood_windows.py`

## 📋 Available Launchers

| Launcher | Description | Compatibility |
|----------|-------------|---------------|
| `run_windows.bat` | **Windows-compatible version (RECOMMENDED)** | ✅ All Windows versions |
| `run_simple.bat` | Simple text-based interface | ✅ All Windows versions |
| `run.bat` | Full version with fallbacks | ⚠️ May have issues on some systems |

## 🔧 Troubleshooting

### Issue: "Python not found"
**Solution:**
1. Install Python from [python.org](https://www.python.org/downloads/)
2. During installation, **check "Add Python to PATH"**
3. Restart command prompt
4. Test: `python --version`

### Issue: Rich library errors (WinError 31)
**Solution:**
Use the Windows-compatible version:
```batch
run_windows.bat
```
This version doesn't use rich/questionary libraries.

### Issue: s5cmd not found or crashes
**Solution:**
The Windows version automatically downloads s5cmd:
1. Run `python s3_flood_windows.py`
2. It will detect your Windows architecture
3. Download and install the correct s5cmd version
4. No manual installation required!

### Issue: Encoding problems (кракозябры)
**Solution:**
All batch files now include `chcp 65001` to fix UTF-8 encoding.

### Issue: Console compatibility problems
**Solution:**
1. Try Windows Terminal (recommended): `winget install Microsoft.WindowsTerminal`
2. Use PowerShell instead of Command Prompt
3. Use the Windows-compatible version: `run_windows.bat`

## 📁 File Structure

```
s3Flood/
├── s3_flood_windows.py     # Windows-compatible version (RECOMMENDED)
├── s3_flood_simple.py      # Simple fallback version
├── s3_flood.py             # Full version with rich UI
├── run_windows.bat         # Windows launcher (RECOMMENDED)
├── run_simple.bat          # Simple launcher
├── run.bat                 # Main launcher with fallbacks
├── install.bat             # Windows installer
├── config.yaml             # Configuration file
└── tools/                  # Auto-downloaded s5cmd
    └── s5cmd.exe
```

## ⚙️ Configuration

Edit `config.yaml` or use the built-in configuration menu:

```yaml
s3_urls: ["http://localhost:9000"]
access_key: "minioadmin"
secret_key: "minioadmin"
bucket_name: "test-bucket"
cluster_mode: false
parallel_threads: 5
file_groups:
  small: {max_size_mb: 100, count: 10}
  medium: {max_size_mb: 1024, count: 5}
  large: {max_size_mb: 5120, count: 2}
infinite_loop: true
cycle_delay_seconds: 15
test_files_directory: "./s3_temp_files"
```

## 🎯 Recommended Usage

For best Windows compatibility:

1. **First time setup:**
   ```batch
   install.bat
   ```

2. **Daily usage:**
   ```batch
   run_windows.bat
   ```

3. **If problems occur:**
   ```batch
   run_simple.bat
   ```

## 🐛 Common Error Solutions

### PermissionError [WinError 31]
- **Cause:** Rich library console compatibility issue
- **Solution:** Use `run_windows.bat` (doesn't use rich)

### Exception 0xc0000005 (s5cmd crash)
- **Cause:** Wrong s5cmd architecture or corrupted binary
- **Solution:** Delete `tools/` folder, restart - auto-downloads correct version

### AssertionError (prompt_toolkit)
- **Cause:** questionary library compatibility issue
- **Solution:** Use `run_windows.bat` (doesn't use questionary)

### SyntaxError
- **Cause:** Python version too old
- **Solution:** Install Python 3.7+ from python.org

## 📞 Support

If you still have issues:
1. Try `run_windows.bat` - most compatible version
2. Check Python installation: `python --version`
3. Manual run: `python s3_flood_windows.py`
4. Check the error messages for specific guidance

The Windows-compatible version (`s3_flood_windows.py`) is designed to work on all Windows systems without external library dependencies.