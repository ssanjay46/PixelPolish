@echo off
title Distributed Task Queue - Primary Server
echo ====================================================================
echo Starting Distributed Task Queue Primary Server
echo Sathyabama Institute of Science and Technology - CSE PT1
echo Server Dashboard: http://localhost:8000
echo Swagger API Docs:  http://localhost:8000/docs
echo ====================================================================
echo.

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found. Please ensure .venv is initialized.
    pause
    exit /b 1
)

.\.venv\Scripts\python.exe -m uvicorn server.app:app --host 0.0.0.0 --port 8000 --reload
pause
