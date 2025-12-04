# Start Spark Cluster and Run Application
# This script starts the master, multiple workers, and runs spark-basic.py

param(
    [int]$NumWorkers = 2
)

$SPARK_HOME = "C:\spark\spark-3.5.7-bin-hadoop3"
$SCRIPT_DIR = $PSScriptRoot

Write-Host "Starting Spark Master..." -ForegroundColor Green
$masterJob = Start-Job -ScriptBlock {
    param($sparkHome)
    Set-Location "$sparkHome\bin"
    & "$sparkHome\bin\spark-class" org.apache.spark.deploy.master.Master --host localhost
} -ArgumentList $SPARK_HOME

# Wait for master to start
Start-Sleep -Seconds 5

Write-Host "Starting $NumWorkers Spark Worker(s)..." -ForegroundColor Green
$workerJobs = @()
for ($i = 1; $i -le $NumWorkers; $i++) {
    $workerJob = Start-Job -ScriptBlock {
        param($sparkHome)
        Set-Location "$sparkHome\bin"
        & "$sparkHome\bin\spark-class" org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost
    } -ArgumentList $SPARK_HOME
    $workerJobs += $workerJob
    Write-Host "  Worker $i started (Job ID: $($workerJob.Id))" -ForegroundColor Cyan
    Start-Sleep -Seconds 2
}

# Wait for workers to register
Write-Host "Waiting for workers to register with master..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "`nSpark Cluster is ready!" -ForegroundColor Green
Write-Host "Master UI: http://localhost:8080" -ForegroundColor Cyan
Write-Host "`nRunning spark-basic.py..." -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Gray

# Activate poetry environment and run the Python script
Set-Location $SCRIPT_DIR
& poetry run python spark-basic.py

Write-Host "`n" + ("=" * 50) -ForegroundColor Gray
Write-Host "spark-basic.py execution completed" -ForegroundColor Green

# Keep cluster running
Write-Host "`nSpark cluster is still running." -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop all Spark processes and exit..." -ForegroundColor Yellow

try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    Write-Host "`nStopping Spark cluster..." -ForegroundColor Red
    
    # Stop all jobs
    $masterJob | Stop-Job | Remove-Job
    $workerJobs | Stop-Job | Remove-Job
    
    # Kill any remaining Spark processes
    Get-Process | Where-Object { $_.ProcessName -like "*java*" -and $_.CommandLine -like "*spark*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    
    Write-Host "Spark cluster stopped." -ForegroundColor Red
}
