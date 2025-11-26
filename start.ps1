# YouTube Analyzer - Start Script
# Starts the Streamlit app

Write-Host "YouTube Analyzer - Starting..." -ForegroundColor Cyan
Write-Host ""

poetry run streamlit run app/main.py
