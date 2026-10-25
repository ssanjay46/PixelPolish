@echo off
title Distributed Task Queue - Automated Demonstration
echo ====================================================================
echo Starting Distributed Task Queue Automated Demo
echo ====================================================================
echo.

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found. Please ensure .venv is initialized.
    pause
    exit /b 1
)

.\.venv\Scripts\python.exe client\demo_transcoding.py
echo.
pause
