# YouTube Analyzer - Start Streamlit Only
# This script starts the Streamlit app, assuming Spark cluster is already running
# Run start-spark-only.ps1 first to start the Spark cluster

param(
    [int]$NumWorkers = 0  # 0 = auto-detect optimal configuration (4 cores per worker)
)

$SCRIPT_DIR = $PSScriptRoot

# Auto-detect and optimize CPU configuration if NumWorkers is 0
if ($NumWorkers -eq 0) {
    $CORES = (Get-CimInstance -ClassName Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum

    # Reserve cores for OS and Spark driver
    $ReservedCores = 4  # Fixed: Reserve 4 cores for OS/driver
    $AvailableCores = $CORES - $ReservedCores

    # Optimal configuration: 4-5 cores per worker
    # Target 4 cores per worker for best performance
    $CoresPerWorker = 4

    # Calculate number of workers based on available cores
    $NumWorkers = [Math]::Floor($AvailableCores / $CoresPerWorker)

    # Ensure at least 1 worker
    if ($NumWorkers -lt 1) {
        $NumWorkers = 1
        $CoresPerWorker = $AvailableCores
    }

    # Memory per worker: 2-3GB per core (with minimum 4GB)
    $MemoryPerCore = 2  # GB
    $WorkerMemoryGB = [Math]::Max(4, [Math]::Ceiling($CoresPerWorker * $MemoryPerCore))
    $WorkerMemory = "$($WorkerMemoryGB)g"

    $TotalAllocatedCores = $NumWorkers * $CoresPerWorker

    Write-Host "Auto-detected $CORES CPU cores" -ForegroundColor Gray
    Write-Host "Optimized configuration: $NumWorkers workers x $CoresPerWorker cores x $WorkerMemory memory" -ForegroundColor Cyan
    Write-Host "Total allocated: $TotalAllocatedCores cores, reserved $ReservedCores for OS/driver" -ForegroundColor Gray
} else {
    # Manual configuration fallback
    $CoresPerWorker = 4
    $WorkerMemory = "5g"
    Write-Host "Using manual configuration: $NumWorkers workers x $CoresPerWorker cores" -ForegroundColor Gray
}

# Load SPARK_HOME from OS environment variable, with fallback to default
if ($env:SPARK_HOME) {
    $SPARK_HOME = $env:SPARK_HOME
} else {
    $SPARK_HOME = "C:\spark\spark-3.5.7-bin-hadoop3"
}

# Check if Spark home exists
if (-not (Test-Path $SPARK_HOME)) {
    Write-Host "ERROR: SPARK_HOME not found at: $SPARK_HOME" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please set SPARK_HOME using one of these methods:" -ForegroundColor Yellow
    Write-Host "  1. Set OS environment variable: `$env:SPARK_HOME = 'C:\path\to\spark'" -ForegroundColor Yellow
    Write-Host "  2. Edit this script and update the default value (line 14)" -ForegroundColor Yellow
    exit 1
}

# Check available memory
$TotalMemoryGB = [Math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
$MemoryGB = [int]($WorkerMemory -replace '[^0-9]', '')
$RequiredMemoryGB = ($NumWorkers * $MemoryGB) + 4  # Workers + 4GB for OS/driver
Write-Host "System Memory: $($TotalMemoryGB)GB total, $($RequiredMemoryGB)GB required" -ForegroundColor Gray

if ($TotalMemoryGB -lt $RequiredMemoryGB) {
    Write-Host ""
    Write-Host "WARNING: Insufficient memory!" -ForegroundColor Yellow
    Write-Host "  Available: $($TotalMemoryGB)GB" -ForegroundColor Yellow
    $WorkerMemoryTotal = $NumWorkers * $MemoryGB
    Write-Host "  Required:  $($RequiredMemoryGB)GB - $WorkerMemoryTotal GB for workers + 4GB for OS" -ForegroundColor Yellow
    Write-Host ""

    # Reduce number of workers (keeping min 4GB per worker)
    $AvailableForWorkers = [Math]::Max(4, [Math]::Floor($TotalMemoryGB - 4))
    $NewNumWorkers = [Math]::Max(1, [Math]::Floor($AvailableForWorkers / 4))  # Min 4GB per worker

    if ($NewNumWorkers -lt $NumWorkers) {
        Write-Host "Reducing to $NewNumWorkers workers (minimum 4GB per worker required)" -ForegroundColor Cyan
        $NumWorkers = $NewNumWorkers
        $WorkerMemory = "4g"
        Write-Host ""
    } else {
        Write-Host "Proceeding anyway - monitor system stability" -ForegroundColor Yellow
        Write-Host ""
    }
}

# Set environment variables locally for Streamlit
$env:SPARK_MODE = "standalone"
$env:SPARK_MASTER_URL = "spark://localhost:7077"
$env:SPARK_WORKER_MEMORY = $WorkerMemory

Write-Host ""
Write-Host "Environment variables set for Streamlit:" -ForegroundColor Yellow
Write-Host "  SPARK_MODE: $env:SPARK_MODE" -ForegroundColor Cyan
Write-Host "  SPARK_MASTER_URL: $env:SPARK_MASTER_URL" -ForegroundColor Cyan
Write-Host "  SPARK_WORKER_MEMORY: $env:SPARK_WORKER_MEMORY" -ForegroundColor Cyan
Write-Host ""

# Verify Spark cluster is accessible
Write-Host "Verifying Spark cluster connectivity..." -ForegroundColor Gray
try {
    # Simple test: try to connect to Spark master UI
    $response = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 5 -ErrorAction Stop
    Write-Host "  Spark Master UI accessible" -ForegroundColor Green
} catch {
    Write-Host "  WARNING: Cannot connect to Spark Master UI (http://localhost:8080)" -ForegroundColor Yellow
    Write-Host "  Make sure Spark cluster is running" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "YouTube Analyzer - Starting Streamlit Only" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host ""

# Display current Spark configuration
Write-Host "Spark Configuration:" -ForegroundColor Green
Write-Host "  Mode:      $env:SPARK_MODE" -ForegroundColor Cyan
Write-Host "  Master:    $env:SPARK_MASTER_URL" -ForegroundColor Cyan
Write-Host "  Home:      $env:SPARK_HOME" -ForegroundColor Cyan
Write-Host "  Memory:    $env:SPARK_WORKER_MEMORY" -ForegroundColor Cyan
Write-Host ""

# Start Streamlit in the background
Write-Host "Starting YouTube Analyzer (Streamlit)..." -ForegroundColor Green
Write-Host ""
Write-Host "=" * 70 -ForegroundColor Gray

$appJob = Start-Job -ScriptBlock {
    param($scriptDir)
    Set-Location $scriptDir
    # Environment variables are inherited from parent session
    # Redirect stderr to stdout so all output is captured together
    & poetry run streamlit run app/Home.py 2>&1
} -ArgumentList $SCRIPT_DIR

# Wait a moment for Streamlit to start and capture initial output
Start-Sleep -Seconds 3

# Display any buffered output in blue
Receive-Job -Id $appJob.Id | ForEach-Object {
    Write-Host $_ -ForegroundColor Blue
}

Write-Host ""
Write-Host "=" * 70 -ForegroundColor Gray
Write-Host ""
Write-Host "Streamlit is running!" -ForegroundColor Green
Write-Host ""
Write-Host "Access Point:" -ForegroundColor Yellow
Write-Host "  - YouTube Analyzer:  http://localhost:8501" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop Streamlit (Spark cluster will continue running)..." -ForegroundColor Yellow
Write-Host ""

# Monitor the Streamlit app output
try {
    while ($true) {
        # Check if app job is still running
        $appState = Get-Job -Id $appJob.Id
        if ($appState.State -ne "Running") {
            Write-Host "Streamlit app has stopped unexpectedly" -ForegroundColor Red
            break
        }

        # Receive and display any output from the app in blue
        Receive-Job -Id $appJob.Id | ForEach-Object {
            Write-Host $_ -ForegroundColor Blue
        }

        Start-Sleep -Seconds 1
    }
} finally {
    Write-Host ""
    Write-Host "=" * 70 -ForegroundColor Red
    Write-Host "Shutting down Streamlit..." -ForegroundColor Red
    Write-Host "=" * 70 -ForegroundColor Red
    Write-Host ""

    # Stop Streamlit app
    Write-Host "Stopping Streamlit app..." -ForegroundColor Yellow
    Stop-Job -Job $appJob -ErrorAction SilentlyContinue
    Remove-Job -Job $appJob -Force -ErrorAction SilentlyContinue

    # Kill any remaining Streamlit processes
    Write-Host "Cleaning up Streamlit processes..." -ForegroundColor Yellow
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "python.exe" -and $_.CommandLine -like "*streamlit*"
    } | ForEach-Object {
        Write-Host "  Force killing Streamlit process $($_.ProcessId)" -ForegroundColor Gray
        & taskkill /F /T /PID $_.ProcessId 2>$null | Out-Null
    }

    Write-Host ""
    Write-Host "Streamlit stopped. Spark cluster is still running." -ForegroundColor Green
    Write-Host "To restart Streamlit: .\start-streamlit-only.ps1" -ForegroundColor Yellow
    Write-Host "To stop Spark: Stop the start-spark-only.ps1 script" -ForegroundColor Yellow
    Write-Host ""
}