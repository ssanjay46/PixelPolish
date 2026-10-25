@echo off
title Distributed Task Queue - Python Worker Node
echo ====================================================================
echo Starting Python Worker Node
echo Connecting to Primary Server...
echo ====================================================================
echo.

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found. Please ensure .venv is initialized.
    pause
    exit /b 1
)

.\.venv\Scripts\python.exe workers\python_worker\worker.py %*
pause
