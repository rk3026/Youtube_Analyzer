# YouTube Analyzer - Quick Start Script
# This script starts the Streamlit app with proper configuration

param(
    [string]$Profile = "",
    [switch]$Help
)

if ($Help) {
    Write-Host @"
YouTube Analyzer - Quick Start Script

USAGE:
  .\start.ps1 -Help              # Show this help message

REQUIREMENTS:
  - Poetry installed (https://python-poetry.org/)
  - Python 3.11+ (managed by Poetry)
  - Java 17 configured in JAVA_HOME
  - MongoDB instance running (local or Atlas)

CONFIGURATION:
  Edit .env file to configure:
  - MongoDB connection settings
  - Spark settings
  - Team member profiles

ENVIRONMENT VARIABLES:
  Optionally create .env file for sensitive credentials:
  - Copy .env.example to .env
  - Uncomment and fill in your values
  - The script will load them automatically

"@
    exit 0
}

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "   YouTube Analyzer - Network Statistics GUI" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# Check Poetry
Write-Host "Checking Poetry installation..." -ForegroundColor Yellow
try {
    $poetryVersion = poetry --version 2>&1
    Write-Host "[OK] Found: $poetryVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Poetry not found! Please install Poetry first" -ForegroundColor Red
    Write-Host "  Install: (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -" -ForegroundColor Yellow
    exit 1
}

# Check Java
Write-Host "Checking Java installation..." -ForegroundColor Yellow
try {
    $javaVersion = java -version 2>&1 | Select-Object -First 1
    Write-Host "[OK] Found: $javaVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Java not found! Please install Java 17" -ForegroundColor Red
    Write-Host "  Download from: https://adoptium.net/temurin/releases/" -ForegroundColor Yellow
    exit 1
}

# Load .env file if exists
if (Test-Path ".env") {
    Write-Host "Loading environment variables from .env..." -ForegroundColor Yellow
    Get-Content .env | ForEach-Object {
        if ($_ -match '^([^#].+?)=(.+)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
            Write-Host "  [OK] Loaded: $name" -ForegroundColor Green
        }
    }
}

# Set profile if specified
if ($Profile) {
    Write-Host "Setting profile to: $Profile" -ForegroundColor Yellow
    $env:YOUTUBE_ANALYZER_PROFILE = $Profile
}

# Display configuration
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Cyan
Write-Host "  Profile: $(if ($env:YOUTUBE_ANALYZER_PROFILE) { $env:YOUTUBE_ANALYZER_PROFILE } else { 'default' })" -ForegroundColor White
Write-Host "  Config: .env file (or environment variables)" -ForegroundColor White
Write-Host ""

# Start Streamlit
Write-Host "Starting Streamlit app with Poetry..." -ForegroundColor Yellow
Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  App will open in your browser shortly..." -ForegroundColor Cyan
Write-Host "  Press Ctrl+C to stop the app" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

poetry run streamlit run app/main.py
