@echo off
title Distributed Task Queue - Java Worker Node
echo ====================================================================
echo Compiling and Starting Java Worker Node (Java 26)
echo ====================================================================
echo.

where javac >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] javac compiler not found in PATH.
    pause
    exit /b 1
)

echo [*] Compiling JavaWorker.java...
javac -d . workers\java_worker\JavaWorker.java
if %errorlevel% neq 0 (
    echo [ERROR] Compilation failed!
    pause
    exit /b 1
)

echo [OK] Compiled successfully. Launching Java Worker Node...
java workers.java_worker.JavaWorker %*
pause
