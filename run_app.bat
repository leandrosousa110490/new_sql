@echo off
REM DuckDB SQL GUI Launcher Script for Windows
REM This script helps set up and run the application with proper error checking

echo ========================================
echo DuckDB SQL GUI Application Launcher
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://python.org
    echo Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)

echo Python found: 
python --version
echo.

REM Check if we're in the right directory
if not exist "main.py" (
    echo ERROR: main.py not found in current directory
    echo Please run this script from the application folder
    pause
    exit /b 1
)

if not exist "requirements.txt" (
    echo ERROR: requirements.txt not found
    echo Please make sure all application files are present
    pause
    exit /b 1
)

echo Checking dependencies...
echo.

REM Try to import required modules
python -c "import PyQt6" >nul 2>&1
if %errorlevel% neq 0 (
    echo PyQt6 not found. Installing dependencies...
    echo.
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo ERROR: Failed to install dependencies
        echo Try running: pip install --upgrade pip
        echo Then: pip install -r requirements.txt
        pause
        exit /b 1
    )
) else (
    echo Dependencies appear to be installed.
)

echo.
echo Starting DuckDB SQL GUI...
echo.
echo If the application doesn't start, check the error messages above.
echo Press Ctrl+C to stop the application.
echo.

REM Run the application
python main.py

REM Check exit code
if %errorlevel% neq 0 (
    echo.
    echo Application exited with error code: %errorlevel%
    echo Check the error messages above for troubleshooting.
    echo.
    echo Common solutions:
    echo 1. Install missing dependencies: pip install -r requirements.txt
    echo 2. Update Python to version 3.8 or higher
    echo 3. Check SETUP_GUIDE.md for detailed troubleshooting
    pause
)

echo.
echo Application closed.
pause