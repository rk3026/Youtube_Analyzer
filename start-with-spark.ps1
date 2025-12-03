# YouTube Analyzer with Spark Standalone Cluster
# This script starts the Spark cluster (master + workers) and the Streamlit app

param(
    [int]$NumWorkers = 2
)

$SCRIPT_DIR = $PSScriptRoot

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
$workerJobs = @()
for ($i = 1; $i -le $NumWorkers; $i++) {
    $workerJob = Start-Job -ScriptBlock {
        param($sparkHome)
        Set-Location "$sparkHome\bin"
        & "$sparkHome\bin\spark-class" org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost
    } -ArgumentList $SPARK_HOME
    $workerJobs += $workerJob
    Write-Host "      Worker $i started (Job ID: $($workerJob.Id))" -ForegroundColor Gray
    Start-Sleep -Seconds 2
}

# Wait for workers to register with master
Write-Host "      Waiting for workers to register with master..." -ForegroundColor Gray
Start-Sleep -Seconds 5

# Step 3: Verify cluster is ready
Write-Host ""
Write-Host "[3/4] Spark Cluster Status:" -ForegroundColor Green
Write-Host "      Master UI:  http://localhost:8080" -ForegroundColor Cyan
Write-Host "      Master URL: spark://localhost:7077" -ForegroundColor Cyan
Write-Host "      Workers:    $NumWorkers" -ForegroundColor Cyan
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
    & poetry run streamlit run app/Home.py
} -ArgumentList $SCRIPT_DIR

# Wait a moment for Streamlit to start
Start-Sleep -Seconds 3

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

        # Receive and display any output from the app
        Receive-Job -Id $appJob.Id | ForEach-Object {
            Write-Host $_
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
    $appJob | Stop-Job | Remove-Job

    # Stop Spark workers
    Write-Host "Stopping Spark workers..." -ForegroundColor Yellow
    $workerJobs | Stop-Job | Remove-Job

    # Stop Spark master
    Write-Host "Stopping Spark master..." -ForegroundColor Yellow
    $masterJob | Stop-Job | Remove-Job

    # Kill any remaining Spark processes
    Write-Host "Cleaning up any remaining Spark processes..." -ForegroundColor Yellow
    Get-Process | Where-Object {
        $_.ProcessName -like "*java*" -and $_.CommandLine -like "*spark*"
    } | Stop-Process -Force -ErrorAction SilentlyContinue

    Write-Host ""
    Write-Host "All services stopped." -ForegroundColor Green
    Write-Host ""
}
