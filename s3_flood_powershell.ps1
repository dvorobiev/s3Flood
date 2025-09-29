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
Write-Log "Starting S3 Load Test (Batch Upload/Download Algorithm). / Запуск нагрузочного тестирования S3 (Алгоритм пакетной загрузки/скачивания)."
Write-Log "Press Ctrl+C to stop. / Нажмите Ctrl+C для остановки."

$rclonePath = Get-Command .\rclone -ErrorAction SilentlyContinue
if (-not $rclonePath) { Write-Log "ERROR: '.\rclone' not found. / ОШИБКА: '.\rclone' не найден." -ForegroundColor Red; exit }
$rcloneFullPath = $rclonePath.Source
Write-Log "Found rclone at: $rcloneFullPath / Найден rclone в: $rcloneFullPath"

# --- Infinite Loop / Бесконечный цикл ---
while ($true) {
    
    # Prepare files for the new cycle
    $fileList = Generate-File-List
    $fullLocalTempDir = (Resolve-Path -Path $localTempDir).Path

    # --- STAGES 1 & 2: Process in batches (Upload -> Download) ---
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

    # --- CYCLE END ---
    Write-Log "--- FULL CYCLE COMPLETE. Restarting in 15 seconds. --- / --- ПОЛНЫЙ ЦИКЛ ЗАВЕРШЕН. Перезапуск через 15 секунд. ---" -ForegroundColor Magenta
    Start-Sleep -Seconds 15
}