# YouTube Analyzer - Start Spark Cluster Only
# This script starts the Spark cluster (master + workers) and sets up environment variables
# Streamlit is NOT started - run start-streamlit-only.ps1 separately

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

Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "YouTube Analyzer - Starting Spark Cluster Only" -ForegroundColor Cyan
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

Write-Host ""

# Track all Spark process IDs for cleanup
$sparkProcessIds = @()

# Helper function to capture Spark process with retry
function Get-SparkProcessWithRetry {
    param(
        [string]$Pattern,
        [int]$MaxRetries = 5,
        [int]$RetryDelaySeconds = 2
    )

    for ($retry = 1; $retry -le $MaxRetries; $retry++) {
        $processes = Get-CimInstance Win32_Process | Where-Object {
            $_.Name -eq "java.exe" -and $_.CommandLine -like $Pattern -and $_.ProcessId -notin $sparkProcessIds
        }

        if ($processes) {
            # Return all matching processes (parent and children)
            return $processes
        }

        if ($retry -lt $MaxRetries) {
            Start-Sleep -Seconds $RetryDelaySeconds
        }
    }

    return $null
}

# Step 1: Start Spark Master
Write-Host "[1/3] Starting Spark Master..." -ForegroundColor Green
$masterJob = Start-Job -ScriptBlock {
    param($sparkHome)
    Set-Location "$sparkHome\bin"
    & "$sparkHome\bin\spark-class" org.apache.spark.deploy.master.Master --host localhost
} -ArgumentList $SPARK_HOME

Write-Host "      Master started (Job ID: $($masterJob.Id))" -ForegroundColor Gray

# Wait for master to start and capture its process ID with retry
Write-Host "      Waiting for master to initialize..." -ForegroundColor Gray
$masterProcesses = Get-SparkProcessWithRetry -Pattern "*org.apache.spark.deploy.master.Master*"

if ($masterProcesses) {
    foreach ($proc in $masterProcesses) {
        $sparkProcessIds += $proc.ProcessId
        Write-Host "      Captured master process ID: $($proc.ProcessId)" -ForegroundColor Gray
    }

    # Verify process is still running
    Start-Sleep -Seconds 1
    $stillRunning = Get-Process -Id $masterProcesses[0].ProcessId -ErrorAction SilentlyContinue
    if ($stillRunning) {
        Write-Host "      Master verified and running" -ForegroundColor Green
    } else {
        Write-Host "      WARNING: Master process exited unexpectedly" -ForegroundColor Yellow
    }
} else {
    Write-Host "      WARNING: Could not capture master process ID" -ForegroundColor Yellow
}

Start-Sleep -Seconds 2

# Step 2: Start Spark Workers
Write-Host ""
Write-Host "[2/3] Starting $NumWorkers Spark Worker(s)..." -ForegroundColor Green
Write-Host "      Configuration: $CoresPerWorker core(s) per worker, $WorkerMemory memory per worker" -ForegroundColor Gray
$workerJobs = @()

# Get initial worker count to track new ones
$initialWorkerCount = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "java.exe" -and $_.CommandLine -like "*org.apache.spark.deploy.worker.Worker*"
}).Count

for ($i = 1; $i -le $NumWorkers; $i++) {
    $workerJob = Start-Job -ScriptBlock {
        param($sparkHome, $cores, $memory)
        Set-Location "$sparkHome\bin"
        & "$sparkHome\bin\spark-class" org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost --cores $cores --memory $memory
    } -ArgumentList $SPARK_HOME, $CoresPerWorker, $WorkerMemory
    $workerJobs += $workerJob
    Write-Host "      Worker $i started (Job ID: $($workerJob.Id))" -ForegroundColor Gray

    # Capture worker process with retry
    $newWorkerProcesses = Get-SparkProcessWithRetry -Pattern "*org.apache.spark.deploy.worker.Worker*" -MaxRetries 3 -RetryDelaySeconds 1

    if ($newWorkerProcesses) {
        foreach ($proc in $newWorkerProcesses) {
            $sparkProcessIds += $proc.ProcessId
            Write-Host "      Captured worker $i process ID: $($proc.ProcessId)" -ForegroundColor Gray
        }

        # Verify process is still running
        $stillRunning = Get-Process -Id $newWorkerProcesses[0].ProcessId -ErrorAction SilentlyContinue
        if ($stillRunning) {
            Write-Host "      Worker $i verified and running" -ForegroundColor Gray
        } else {
            Write-Host "      WARNING: Worker $i process exited unexpectedly" -ForegroundColor Yellow
        }
    } else {
        Write-Host "      WARNING: Could not capture worker $i process ID" -ForegroundColor Yellow
    }
}

# Wait for workers to register with master
Write-Host ""
Write-Host "      Waiting for workers to register with master..." -ForegroundColor Gray
Start-Sleep -Seconds 3

    # Validate all tracked processes are still running
    Write-Host "      Validating cluster health..." -ForegroundColor Gray
    $runningCount = 0
    $failedProcesses = @()

    foreach ($processId in $sparkProcessIds) {
        $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($proc) {
            $runningCount++
        } else {
            $failedProcesses += $processId
        }
    }$expectedCount = 1 + $NumWorkers  # 1 master + N workers
if ($runningCount -eq $expectedCount) {
    Write-Host "      [OK] All $runningCount Spark processes verified running" -ForegroundColor Green
} else {
    Write-Host "      [WARN] Expected $expectedCount processes, found $runningCount running" -ForegroundColor Yellow
    if ($failedProcesses.Count -gt 0) {
        Write-Host "      Failed PIDs: $($failedProcesses -join ', ')" -ForegroundColor Yellow
    }
}

Write-Host "      Tracked PIDs: $($sparkProcessIds.Count) total" -ForegroundColor Gray

# Step 3: Verify cluster is ready
$TotalCoresAllocated = $NumWorkers * $CoresPerWorker
# Extract numeric value from WorkerMemory (e.g., "5g" -> 5)
$MemoryGB = [int]($WorkerMemory -replace '[^0-9]', '')
$TotalMemoryAllocated = $NumWorkers * $MemoryGB

Write-Host ""
Write-Host "[3/3] Spark Cluster Status:" -ForegroundColor Green
Write-Host "      Master UI:  http://localhost:8080" -ForegroundColor Cyan
Write-Host "      Master URL: spark://localhost:7077" -ForegroundColor Cyan
Write-Host "      Workers:    $NumWorkers workers" -ForegroundColor Cyan
Write-Host "      Cores:      $TotalCoresAllocated total, $CoresPerWorker per worker" -ForegroundColor Cyan
Write-Host "      Memory:     $($TotalMemoryAllocated)GB total, $WorkerMemory per worker" -ForegroundColor Cyan
Write-Host "      Executors:  $WorkerMemory memory per executor" -ForegroundColor Cyan
Write-Host "      Status:     Ready" -ForegroundColor Green

Write-Host ""
Write-Host "Spark cluster is running!" -ForegroundColor Green
Write-Host ""
Write-Host "To start Streamlit separately, run: .\start-streamlit-only.ps1" -ForegroundColor Yellow
Write-Host ""
Write-Host "Press Ctrl+C to stop Spark cluster..." -ForegroundColor Yellow
Write-Host ""

# Monitor Spark cluster
try {
    while ($true) {
        # Check if master process is still running
        $masterProcess = Get-Process -Id $sparkProcessIds[0] -ErrorAction SilentlyContinue
        if (-not $masterProcess) {
            Write-Host "Spark master has stopped unexpectedly" -ForegroundColor Red
            break
        }

        # Check if all worker processes are still running
        $runningWorkers = 0
        foreach ($workerPid in $sparkProcessIds[1..($sparkProcessIds.Count-1)]) {
            $workerProcess = Get-Process -Id $workerPid -ErrorAction SilentlyContinue
            if ($workerProcess) {
                $runningWorkers++
            }
        }

        if ($runningWorkers -ne $NumWorkers) {
            Write-Host "Some Spark workers have stopped unexpectedly ($runningWorkers/$NumWorkers running)" -ForegroundColor Red
            break
        }

        Start-Sleep -Seconds 5
    }
} finally {
    Write-Host ""
    Write-Host "=" * 70 -ForegroundColor Red
    Write-Host "Shutting down Spark cluster..." -ForegroundColor Red
    Write-Host "=" * 70 -ForegroundColor Red
    Write-Host ""

    # Stop PowerShell background jobs
    Write-Host "Stopping Spark background jobs..." -ForegroundColor Yellow
    Stop-Job -Job $masterJob -ErrorAction SilentlyContinue
    Remove-Job -Job $masterJob -Force -ErrorAction SilentlyContinue

    $workerJobs | ForEach-Object {
        Stop-Job -Job $_ -ErrorAction SilentlyContinue
        Remove-Job -Job $_ -Force -ErrorAction SilentlyContinue
    }

    # Kill tracked Spark processes by PID (using taskkill for force termination with child processes)
    Write-Host "Killing tracked Spark processes..." -ForegroundColor Yellow
    foreach ($processId in $sparkProcessIds) {
        try {
            $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
            if ($process) {
                Write-Host "  Force killing process tree $processId ($($process.Name))" -ForegroundColor Gray
                # Use taskkill /F /T to kill process and all children
                & taskkill /F /T /PID $processId 2>$null | Out-Null
            }
        } catch {
            # Process may have already exited
        }
    }

    # Safety net: kill any remaining Spark-related processes we might have missed
    Write-Host "Final cleanup sweep..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2

    # Kill all Spark-related Java processes
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "java.exe" -and $_.CommandLine -like "*org.apache.spark*"
    } | ForEach-Object {
        Write-Host "  Force killing Java process $($_.ProcessId)" -ForegroundColor Gray
        & taskkill /F /T /PID $_.ProcessId 2>$null | Out-Null
    }

    # Kill all spark-class cmd processes
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "cmd.exe" -and $_.CommandLine -like "*spark-class*"
    } | ForEach-Object {
        Write-Host "  Force killing cmd process $($_.ProcessId)" -ForegroundColor Gray
        & taskkill /F /T /PID $_.ProcessId 2>$null | Out-Null
    }

    # Kill any orphaned console host processes (conhost.exe) that might be lingering
    Start-Sleep -Seconds 1
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "conhost.exe" -and $_.CommandLine -like "*java.exe*"
    } | ForEach-Object {
        Write-Host "  Killing console host $($_.ProcessId)" -ForegroundColor Gray
        & taskkill /F /PID $_.ProcessId 2>$null | Out-Null
    }

    Write-Host ""
    Write-Host "Spark cluster stopped." -ForegroundColor Green
    Write-Host ""
}