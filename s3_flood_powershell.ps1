# ===================================================================
# Parallel S3 Load Test Script - BATCH UPLOAD/DOWNLOAD VERSION
# Параллельный скрипт нагрузочного тестирования S3 - ВЕРСИЯ ЗАГРУЗКИ/СКАЧИВАНИЯ ПАКЕТАМИ
# Algorithm: Upload 10 -> Download 10 -> Repeat for all. Then delete all.
# Алгоритм: Загрузить 10 -> Скачать 10 -> Повторить для всех. Затем удалить все.
# ===================================================================

# --- CONFIGURATION / НАСТРОЙКА ---
$rcloneRemote = "demo" 
$bucketName = "backup"
$localTempDir = ".\S3_TEMP_FILES"

# Batch size and max parallel jobs
# Размер пакета и максимальное количество параллельных задач
$batchSize = 10

# Algorithm selection
# Выбор алгоритма
# 0 = Traditional (Write-Read-Delete)
# 1 = Infinite Write (Write only, no deletion)
$algorithm = 0
# ---------------------


# --- Helper Functions / Вспомогательные функции ---
function Write-Log {
    param ([string]$Message, [System.ConsoleColor]$ForegroundColor = 'Gray')
    Write-Host ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message) -ForegroundColor $ForegroundColor
}

function Create-Test-File {
    param ([string]$filePath, [long]$fileSizeInBytes)
    fsutil file createnew $filePath $fileSizeInBytes | Out-Null
}

function Generate-File-List {
    Write-Log "Preparing 100 test files in `"$localTempDir`"... / Подготовка 100 тестовых файлов в `"$localTempDir`"..."
    if (Test-Path $localTempDir) { Remove-Item -Path $localTempDir -Recurse -Force }
    $fullTempDirPath = New-Item -Path $localTempDir -ItemType Directory -Force
    
    $fileList = @() 
    
    1..30 | ForEach-Object { $size = (Get-Random -Minimum 1 -Maximum 100) * 1MB; $path = Join-Path $fullTempDirPath.FullName "small_$_-$(New-Guid).dat"; Create-Test-File -filePath $path -fileSizeInBytes $size; $fileList += $path }
    1..30 | ForEach-Object { $size = (Get-Random -Minimum 101 -Maximum 1024) * 1MB; $path = Join-Path $fullTempDirPath.FullName "medium_$_-$(New-Guid).dat"; Create-Test-File -filePath $path -fileSizeInBytes $size; $fileList += $path }
    1..30 | ForEach-Object { $size = (Get-Random -Minimum 1 -Maximum 10) * 1GB; $path = Join-Path $fullTempDirPath.FullName "large_$_-$(New-Guid).dat"; Create-Test-File -filePath $path -fileSizeInBytes $size; $fileList += $path }
    1..10 | ForEach-Object { $size = (Get-Random -Minimum 11 -Maximum 100) * 1GB; $path = Join-Path $fullTempDirPath.FullName "huge_$_-$(New-Guid).dat"; Create-Test-File -filePath $path -fileSizeInBytes $size; $fileList += $path }

    Write-Log "Created $($fileList.Count) test files. / Создано $($fileList.Count) тестовых файлов."
    return $fileList | Get-Random -Count $fileList.Count
}

function Select-Algorithm {
    Write-Host ""
    Write-Host "Select Algorithm / Выберите алгоритм:" -ForegroundColor Cyan
    Write-Host "1. Traditional (Write-Read-Delete) / Традиционный (Запись-Чтение-Удаление)" -ForegroundColor Yellow
    Write-Host "   Upload files -> Read files -> Delete files" -ForegroundColor Gray
    Write-Host "   Загрузка файлов -> Чтение файлов -> Удаление файлов" -ForegroundColor Gray
    Write-Host "2. Infinite Write (Write only, no deletion) / Бесконечная запись (только запись, без удаления)" -ForegroundColor Yellow
    Write-Host "   Continuously upload files without deletion" -ForegroundColor Gray
    Write-Host "   Непрерывная загрузка файлов без удаления" -ForegroundColor Gray
    Write-Host ""
    
    do {
        $choice = Read-Host "Enter your choice (1 or 2) / Введите ваш выбор (1 или 2)"
        switch ($choice) {
            "1" { 
                $script:algorithm = 0
                Write-Host "Selected: Traditional (Write-Read-Delete) / Выбран: Традиционный (Запись-Чтение-Удаление)" -ForegroundColor Green
                return
            }
            "2" { 
                $script:algorithm = 1
                Write-Host "Selected: Infinite Write (Write only, no deletion) / Выбран: Бесконечная запись (только запись, без удаления)" -ForegroundColor Green
                return
            }
            default { 
                Write-Host "Invalid choice. Please enter 1 or 2. / Неверный выбор. Пожалуйста, введите 1 или 2." -ForegroundColor Red
            }
        }
    } while ($true)
}

# --- MAIN LOGIC / ОСНОВНАЯ ЛОГИКА ---

trap {
    Write-Log "Stopping signal received. Cleaning up... / Получен сигнал остановки. Очистка..." -ForegroundColor Yellow
    Get-Job | Stop-Job -PassThru | Remove-Job -Force
    if (Test-Path $localTempDir) {
        Write-Log "Deleting temporary directory `"$localTempDir`"... / Удаление временной директории `"$localTempDir`"..."
        Remove-Item -Path $localTempDir -Recurse -Force
    }
    Write-Log "Script finished. / Скрипт завершен."
    exit
}

Clear-Host
Write-Log "S3 Flood v1.7.0 - PowerShell Edition / S3 Flood v1.7.0 - PowerShell версия"
Write-Log "========================================================================"

# Select algorithm
Select-Algorithm

Write-Log "Press Ctrl+C to stop. / Нажмите Ctrl+C для остановки."

$rclonePath = Get-Command .\rclone -ErrorAction SilentlyContinue
if (-not $rclonePath) { Write-Log "ERROR: '.\rclone' not found. / ОШИБКА: '.\rclone' не найден." -ForegroundColor Red; exit }
$rcloneFullPath = $rclonePath.Source
Write-Log "Found rclone at: $rcloneFullPath / Найден rclone в: $rcloneFullPath"

# --- Infinite Loop / Бесконечный цикл ---
$cycleCount = 0
while ($true) {
    $cycleCount++
    Write-Log "--- STARTING CYCLE $cycleCount --- / --- НАЧАЛО ЦИКЛА $cycleCount ---" -ForegroundColor Cyan
    
    # Prepare files for the new cycle
    $fileList = Generate-File-List
    $fullLocalTempDir = (Resolve-Path -Path $localTempDir).Path

    if ($algorithm -eq 0) {
        # --- TRADITIONAL ALGORITHM: Process in batches (Upload -> Download -> Delete) ---
        Write-Log "Using Traditional Algorithm (Write-Read-Delete) / Используется традиционный алгоритм (Запись-Чтение-Удаление)" -ForegroundColor Blue
        
        $batchCount = [math]::Ceiling($fileList.Count / $batchSize)
        for ($i = 0; $i -lt $fileList.Count; $i += $batchSize) {
            $currentBatchNum = ($i / $batchSize) + 1
            $endIndex = [System.Math]::Min($i + $batchSize - 1, $fileList.Count - 1)
            $batch = $fileList[$i..$endIndex]
            
            Write-Log "--- Processing Batch $currentBatchNum of $batchCount ($($batch.Count) files) --- / --- Обработка пакета $currentBatchNum из $batchCount ($($batch.Count) файлов) ---" -ForegroundColor Cyan

            # --- UPLOAD BATCH ---
            Write-Log "[UPLOAD] Starting jobs for batch $currentBatchNum... / [ЗАГРУЗКА] Запуск задач для пакета $currentBatchNum..."
            foreach ($localFilePath in $batch) {
                $fileName = Split-Path -Leaf $localFilePath
                $s3Path = "$($rcloneRemote):$($bucketName)/$($fileName)"
                Start-Job -Name "UPLOAD_$fileName" -ScriptBlock { 
                    param($exePath, $lPath, $rPath)
                    & $exePath copy `"$lPath`" `"$($rPath | Split-Path -Parent)`" --progress --no-traverse 
                } -ArgumentList $rcloneFullPath, $localFilePath, $s3Path
            }
            Write-Log "[UPLOAD] Jobs started. Waiting for batch to complete... / [ЗАГРУЗКА] Задачи запущены. Ожидание завершения пакета..."
            Get-Job | Wait-Job | Out-Null
            Write-Log "[UPLOAD] Batch $currentBatchNum uploaded successfully. / [ЗАГРУЗКА] Пакет $currentBatchNum успешно загружен." -ForegroundColor Green
            Get-Job | Remove-Job
            
            # --- DOWNLOAD BATCH ---
            Write-Log "[DOWNLOAD] Starting jobs for batch $currentBatchNum... / [СКАЧИВАНИЕ] Запуск задач для пакета $currentBatchNum..."
            foreach ($localFilePath in $batch) {
                $fileName = Split-Path -Leaf $localFilePath
                $s3Path = "$($rcloneRemote):$($bucketName)/$($fileName)"
                Start-Job -Name "DOWNLOAD_$fileName" -ScriptBlock { 
                    param($exePath, $rPath, $tempDirPath, $fName)
                    $localDownloadPath = Join-Path $tempDirPath "downloaded_$fName"
                    & $exePath copyto `"$rPath`" `"$localDownloadPath`" --progress
                    Remove-Item -Path $localDownloadPath -Force 
                } -ArgumentList $rcloneFullPath, $s3Path, $fullLocalTempDir, $fileName
            }
            Write-Log "[DOWNLOAD] Jobs started. Waiting for batch to complete... / [СКАЧИВАНИЕ] Задачи запущены. Ожидание завершения пакета..."
            Get-Job | Wait-Job | Out-Null
            Write-Log "[DOWNLOAD] Batch $currentBatchNum downloaded successfully. / [СКАЧИВАНИЕ] Пакет $currentBatchNum успешно скачан." -ForegroundColor Green
            Get-Job | Remove-Job
        }

        # --- STAGE 3: DELETE ALL FILES AFTER PROCESSING ---
        Write-Log "--- STAGE 3: DELETING all $($fileList.Count) files --- / --- ЭТАП 3: УДАЛЕНИЕ всех $($fileList.Count) файлов ---" -ForegroundColor Yellow
        foreach ($localFilePath in $fileList) {
            $fileName = Split-Path -Leaf $localFilePath
            $s3Path = "$($rcloneRemote):$($bucketName)/$($fileName)"
            # Log progress every 10 files to avoid spamming the console
            if (($fileList.IndexOf($localFilePath) % 10) -eq 0) {
                Write-Log "[DELETE] Deleting files... ($($fileList.IndexOf($localFilePath)) of $($fileList.Count)) / [УДАЛЕНИЕ] Удаление файлов... ($($fileList.IndexOf($localFilePath)) из $($fileList.Count))"
            }
            & $rcloneFullPath deletefile `"$s3Path`"
        }
        Write-Log "[DELETE] All files deleted successfully. / [УДАЛЕНИЕ] Все файлы успешно удалены." -ForegroundColor Green
    }
    else {
        # --- INFINITE WRITE ALGORITHM: Continuously upload files without deletion ---
        Write-Log "Using Infinite Write Algorithm (Write only, no deletion) / Используется алгоритм бесконечной записи (только запись, без удаления)" -ForegroundColor Blue
        
        # Upload all files first
        Write-Log "[UPLOAD] Uploading all files... / [ЗАГРУЗКА] Загрузка всех файлов..."
        foreach ($localFilePath in $fileList) {
            $fileName = Split-Path -Leaf $localFilePath
            $s3Path = "$($rcloneRemote):$($bucketName)/$($fileName)"
            Start-Job -Name "UPLOAD_$fileName" -ScriptBlock { 
                param($exePath, $lPath, $rPath)
                & $exePath copy `"$lPath`" `"$($rPath | Split-Path -Parent)`" --progress --no-traverse 
            } -ArgumentList $rcloneFullPath, $localFilePath, $s3Path
        }
        Write-Log "[UPLOAD] All upload jobs started. Waiting for completion... / [ЗАГРУЗКА] Все задачи загрузки запущены. Ожидание завершения..."
        Get-Job | Wait-Job | Out-Null
        Write-Log "[UPLOAD] All files uploaded successfully. / [ЗАГРУЗКА] Все файлы успешно загружены." -ForegroundColor Green
        Get-Job | Remove-Job
        
        # Continuous write operations
        Write-Log "[INFINITE WRITE] Starting continuous write operations... / [БЕСКОНЕЧНАЯ ЗАПИСЬ] Начало непрерывных операций записи..."
        $writeCycle = 0
        while ($writeCycle -lt 5 -and $true) {  # Run 5 write cycles then restart with new files
            $writeCycle++
            Write-Log "[INFINITE WRITE] Write cycle $writeCycle / [БЕСКОНЕЧНАЯ ЗАПИСЬ] Цикл записи $writeCycle"
            
            # Re-upload all files
            foreach ($localFilePath in $fileList) {
                $fileName = Split-Path -Leaf $localFilePath
                $s3Path = "$($rcloneRemote):$($bucketName)/$($fileName)"
                Start-Job -Name "REWRITE_$fileName" -ScriptBlock { 
                    param($exePath, $lPath, $rPath)
                    # Create a temporary file with new content
                    $tempPath = "$lPath.temp"
                    $content = "Rewrite test data - $(Get-Date)" | Out-File -FilePath $tempPath -Encoding ASCII
                    & $exePath copy `"$tempPath`" `"$($rPath | Split-Path -Parent)`" --progress --no-traverse
                    Remove-Item -Path $tempPath -Force
                } -ArgumentList $rcloneFullPath, $localFilePath, $s3Path
            }
            
            Write-Log "[INFINITE WRITE] Re-write jobs started. Waiting for completion... / [БЕСКОНЕЧНАЯ ЗАПИСЬ] Задачи перезаписи запущены. Ожидание завершения..."
            Get-Job | Wait-Job | Out-Null
            Write-Log "[INFINITE WRITE] Write cycle $writeCycle completed. / [БЕСКОНЕЧНАЯ ЗАПИСЬ] Цикл записи $writeCycle завершен." -ForegroundColor Green
            Get-Job | Remove-Job
            
            # Small delay between write cycles
            Start-Sleep -Seconds 2
        }
    }

    # --- CYCLE END ---
    Write-Log "--- CYCLE $cycleCount COMPLETE. Restarting in 15 seconds. --- / --- ЦИКЛ $cycleCount ЗАВЕРШЕН. Перезапуск через 15 секунд. ---" -ForegroundColor Magenta
    Start-Sleep -Seconds 15
}