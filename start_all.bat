@echo off
title Distributed Task Queue - Master Launcher
echo ====================================================================
echo   LAUNCHING DISTRIBUTED TASK QUEUE ECOSYSTEM
echo   Sathyabama Institute of Science and Technology - CSE PT1
echo ====================================================================
echo.
echo [*] Step 1: Starting Primary Server & Web Dashboard...
start "1. Primary Server" cmd /k "run_server.bat"

echo [*] Waiting 3 seconds for server startup...
timeout /t 3 /nobreak >nul

echo [*] Step 2: Starting Python Worker Node...
start "2. Python Worker" cmd /k "run_python_worker.bat"

echo [*] Step 3: Starting Java Worker Node...
start "3. Java Worker" cmd /k "run_java_worker.bat"

echo [*] Step 4: Opening Live Web Dashboard in browser...
start http://127.0.0.1:8000

echo.
echo ====================================================================
echo [OK] All components launched in separate windows!
echo  - Server Dashboard: http://127.0.0.1:8000
echo  - Python Worker: Active (video_transcode + audio_extract + thumbnail_extract + chunk_process)
echo  - Java Worker: Active (media_watermark_hash + matrix_multiply + hash_solve)
echo.
echo Next steps:
echo   - Open the web dashboard to drag-and-drop any MP4 video or image!
echo   - Or run 'run_demo.bat' for automated end-to-end parallel benchmark!
echo ====================================================================
pause
