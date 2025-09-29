# S3 Flood PowerShell Edition - Installation Script
# =================================================

# Function to write colored output
function Write-Log {
    param (
        [string]$Message,
        [System.ConsoleColor]$Color = 'Gray'
    )
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message" -ForegroundColor $Color
}

# Function to download file with progress
function Download-FileWithProgress {
    param (
        [string]$Url,
        [string]$DestinationPath
    )
    
    Write-Log "Downloading $Url..." 'Cyan'
    
    try {
        $ProgressPreference = 'SilentlyContinue'  # Disable progress bar for faster download
        Invoke-WebRequest -Uri $Url -OutFile $DestinationPath -ErrorAction Stop
        Write-Log "Download completed successfully." 'Green'
        return $true
    }
    catch {
        Write-Log "Failed to download: $($_.Exception.Message)" 'Red'
        return $false
    }
    finally {
        $ProgressPreference = 'Continue'
    }
}

# Main installation script
Write-Log "S3 Flood PowerShell Edition - Installation" 'Yellow'
Write-Log "=========================================" 'Yellow'
Write-Log ""

# Check if running as administrator (required for fsutil)
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Log "WARNING: This script should be run as Administrator for full functionality." 'Yellow'
    Write-Log "Some features may not work properly without administrator privileges." 'Yellow'
    Write-Log ""
}

# Detect architecture
$architecture = "unknown"
if ([Environment]::Is64BitOperatingSystem) {
    $architecture = "Windows_x86_64"
    Write-Log "Detected 64-bit system." 'Green'
} else {
    $architecture = "Windows_i386"
    Write-Log "Detected 32-bit system." 'Green'
}

# Create tools directory if it doesn't exist
$toolsDir = ".\tools"
if (-not (Test-Path $toolsDir)) {
    New-Item -ItemType Directory -Path $toolsDir | Out-Null
    Write-Log "Created tools directory." 'Green'
}

# Download rclone
Write-Log "Installing rclone..." 'Cyan'
$rcloneBaseUrl = "https://downloads.rclone.org"
$rcloneVersionUrl = "$rcloneBaseUrl/version.txt"
$rcloneDownloadUrl = ""

try {
    $ProgressPreference = 'SilentlyContinue'
    $rcloneVersion = Invoke-WebRequest -Uri $rcloneVersionUrl -UseBasicParsing | Select-Object -ExpandProperty Content
    $rcloneVersion = $rcloneVersion.Trim()
    Write-Log "Latest rclone version: $rcloneVersion" 'Green'
    
    $rcloneDownloadUrl = "$rcloneBaseUrl/rclone-$rcloneVersion-windows-amd64.zip"
    Write-Log "Download URL: $rcloneDownloadUrl" 'Gray'
} catch {
    Write-Log "Failed to get latest rclone version, using default." 'Yellow'
    $rcloneDownloadUrl = "$rcloneBaseUrl/rclone-current-windows-amd64.zip"
}

# Download and extract rclone
$tempZip = ".\rclone_temp.zip"
if (Download-FileWithProgress -Url $rcloneDownloadUrl -DestinationPath $tempZip) {
    try {
        Write-Log "Extracting rclone..." 'Cyan'
        Expand-Archive -Path $tempZip -DestinationPath "." -Force
        
        # Find the extracted folder (name may vary with version)
        $extractedFolder = Get-ChildItem -Path "." -Directory | Where-Object { $_.Name -like "rclone*" } | Select-Object -First 1
        
        if ($extractedFolder) {
            $rcloneExePath = Join-Path $extractedFolder.FullName "rclone.exe"
            if (Test-Path $rcloneExePath) {
                Move-Item -Path $rcloneExePath -Destination ".\tools\rclone.exe" -Force
                Write-Log "rclone installed successfully to tools\rclone.exe" 'Green'
            } else {
                Write-Log "rclone.exe not found in extracted folder!" 'Red'
            }
        } else {
            Write-Log "Extracted rclone folder not found!" 'Red'
        }
        
        # Cleanup
        Remove-Item -Path $tempZip -Force
        if ($extractedFolder) {
            Remove-Item -Path $extractedFolder.FullName -Recurse -Force
        }
    } catch {
        Write-Log "Failed to extract rclone: $($_.Exception.Message)" 'Red'
    }
} else {
    Write-Log "Failed to download rclone. Please download manually from https://rclone.org/downloads/" 'Red'
}

Write-Log ""
Write-Log "Installation completed!" 'Green'
Write-Log ""
Write-Log "Next steps:" 'Yellow'
Write-Log "1. Configure rclone by running: .\tools\rclone.exe config" 'Yellow'
Write-Log "2. Make sure to create a remote named 'demo' (or update the script)" 'Yellow'
Write-Log "3. Run the test script: .\s3_flood_powershell.ps1" 'Yellow'
Write-Log ""
Write-Log "IMPORTANT: The PowerShell script requires administrator privileges to create large test files." 'Red'