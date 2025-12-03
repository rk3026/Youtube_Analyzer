# YouTube Analyzer with Spark Standalone Cluster
# This script starts the Spark cluster (master + workers) and the Streamlit app

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
    Write-Host "Optimized configuration: $NumWorkers workers × $CoresPerWorker cores × $WorkerMemory memory" -ForegroundColor Cyan
    Write-Host "Total allocated: $TotalAllocatedCores cores (reserved $ReservedCores for OS/driver)" -ForegroundColor Gray
} else {
    # Manual configuration fallback
    $CoresPerWorker = 4
    $WorkerMemory = "5g"
    Write-Host "Using manual configuration: $NumWorkers workers × $CoresPerWorker cores" -ForegroundColor Gray
}

# Load SPARK_HOME from OS environment variable, with fallback to default
if ($env:SPARK_HOME) {
    $SPARK_HOME = $env:SPARK_HOME
} else {
    $SPARK_HOME = "C:\spark\spark-3.5.7-bin-hadoop3"
}

Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "YouTube Analyzer - Starting with Spark Standalone Cluster" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host ""

# Check if Spark home exists
if (-not (Test-Path $SPARK_HOME)) {
    Write-Host "ERROR: SPARK_HOME not found at: $SPARK_HOME" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please set SPARK_HOME using one of these methods:" -ForegroundColor Yellow
    Write-Host "  1. Set OS environment variable: `$env:SPARK_HOME = 'C:\path\to\spark'" -ForegroundColor Yellow
    Write-Host "  2. Edit this script and update the default value (line 14)" -ForegroundColor Yellow
    exit 1
}

# Step 1: Start Spark Master
Write-Host "[1/4] Starting Spark Master..." -ForegroundColor Green
$masterJob = Start-Job -ScriptBlock {
    param($sparkHome)
    Set-Location "$sparkHome\bin"
    & "$sparkHome\bin\spark-class" org.apache.spark.deploy.master.Master --host localhost
} -ArgumentList $SPARK_HOME

Write-Host "      Master started (Job ID: $($masterJob.Id))" -ForegroundColor Gray

# Wait for master to start
Write-Host "      Waiting for master to initialize..." -ForegroundColor Gray
Start-Sleep -Seconds 5

# Step 2: Start Spark Workers
Write-Host ""
Write-Host "[2/4] Starting $NumWorkers Spark Worker(s)..." -ForegroundColor Green
Write-Host "      Configuration: $CoresPerWorker core(s) per worker, $WorkerMemory memory per worker" -ForegroundColor Gray
$workerJobs = @()
for ($i = 1; $i -le $NumWorkers; $i++) {
    $workerJob = Start-Job -ScriptBlock {
        param($sparkHome, $cores, $memory)
        Set-Location "$sparkHome\bin"
        & "$sparkHome\bin\spark-class" org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost --cores $cores --memory $memory
    } -ArgumentList $SPARK_HOME, $CoresPerWorker, $WorkerMemory
    $workerJobs += $workerJob
    Write-Host "      Worker $i started (Job ID: $($workerJob.Id))" -ForegroundColor Gray

    # Add a small delay between worker starts to avoid resource contention
    if ($i -lt $NumWorkers) {
        Start-Sleep -Milliseconds 500
    }
}

# Wait for workers to register with master
Write-Host "      Waiting for workers to register with master..." -ForegroundColor Gray
Start-Sleep -Seconds 5

# Step 3: Verify cluster is ready
$TotalCoresAllocated = $NumWorkers * $CoresPerWorker
# Extract numeric value from WorkerMemory (e.g., "5g" -> 5)
$MemoryGB = [int]($WorkerMemory -replace '[^0-9]', '')
$TotalMemoryAllocated = $NumWorkers * $MemoryGB

Write-Host ""
Write-Host "[3/4] Spark Cluster Status:" -ForegroundColor Green
Write-Host "      Master UI:  http://localhost:8080" -ForegroundColor Cyan
Write-Host "      Master URL: spark://localhost:7077" -ForegroundColor Cyan
Write-Host "      Workers:    $NumWorkers" -ForegroundColor Cyan
Write-Host "      Total Cores: $TotalCoresAllocated ($CoresPerWorker cores per worker)" -ForegroundColor Cyan
Write-Host "      Total Memory: $($TotalMemoryAllocated)GB ($WorkerMemory per worker)" -ForegroundColor Cyan
Write-Host "      Status:     Ready" -ForegroundColor Green

# Step 4: Start Streamlit App
Write-Host ""
Write-Host "[4/4] Starting YouTube Analyzer (Streamlit)..." -ForegroundColor Green
Write-Host ""
Write-Host "=" * 70 -ForegroundColor Gray

# Set environment variables for the app
$env:SPARK_MODE = "standalone"
$env:SPARK_MASTER_URL = "spark://localhost:7077"
$env:SPARK_HOME = $SPARK_HOME

# Start Streamlit in the background
$appJob = Start-Job -ScriptBlock {
    param($scriptDir)
    Set-Location $scriptDir
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
Write-Host "All services are running!" -ForegroundColor Green
Write-Host ""
Write-Host "Access Points:" -ForegroundColor Yellow
Write-Host "  - YouTube Analyzer:  http://localhost:8501" -ForegroundColor Cyan
Write-Host "  - Spark Master UI:   http://localhost:8080" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop all services and exit..." -ForegroundColor Yellow
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
    Write-Host "Shutting down all services..." -ForegroundColor Red
    Write-Host "=" * 70 -ForegroundColor Red
    Write-Host ""

    # Stop Streamlit app
    Write-Host "Stopping Streamlit app..." -ForegroundColor Yellow
    Stop-Job -Job $appJob -ErrorAction SilentlyContinue
    Remove-Job -Job $appJob -Force -ErrorAction SilentlyContinue

    # Stop Spark workers
    Write-Host "Stopping Spark workers..." -ForegroundColor Yellow
    $workerJobs | ForEach-Object {
        Stop-Job -Job $_ -ErrorAction SilentlyContinue
        Remove-Job -Job $_ -Force -ErrorAction SilentlyContinue
    }

    # Stop Spark master
    Write-Host "Stopping Spark master..." -ForegroundColor Yellow
    Stop-Job -Job $masterJob -ErrorAction SilentlyContinue
    Remove-Job -Job $masterJob -Force -ErrorAction SilentlyContinue

    # Kill all Spark-related Java processes
    Write-Host "Cleaning up Spark Java processes..." -ForegroundColor Yellow
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "java.exe" -and $_.CommandLine -like "*spark*"
    } | ForEach-Object {
        Write-Host "  Killing process $($_.ProcessId): $($_.Name)" -ForegroundColor Gray
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }

    # Kill any orphaned cmd.exe processes related to Spark
    Write-Host "Cleaning up command prompt windows..." -ForegroundColor Yellow
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "cmd.exe" -and $_.CommandLine -like "*spark*"
    } | ForEach-Object {
        Write-Host "  Killing process $($_.ProcessId): cmd.exe" -ForegroundColor Gray
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }

    # Final cleanup: kill any remaining java.exe with spark-class in command line
    Start-Sleep -Seconds 1
    Get-Process -Name "java" -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)"
            if ($proc.CommandLine -like "*org.apache.spark*") {
                Write-Host "  Killing remaining Spark process: $($_.Id)" -ForegroundColor Gray
                Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {
            # Process may have already exited
        }
    }

    Write-Host ""
    Write-Host "All services stopped." -ForegroundColor Green
    Write-Host ""
}
