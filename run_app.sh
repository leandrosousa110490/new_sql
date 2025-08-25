#!/bin/bash
# DuckDB SQL GUI Launcher Script for macOS/Linux
# This script helps set up and run the application with proper error checking

echo "========================================"
echo "DuckDB SQL GUI Application Launcher"
echo "========================================"
echo

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if Python is installed
if command_exists python3; then
    PYTHON_CMD="python3"
elif command_exists python; then
    PYTHON_CMD="python"
else
    echo "ERROR: Python is not installed or not in PATH"
    echo "Please install Python 3.8+ using your package manager:"
    echo "  Ubuntu/Debian: sudo apt install python3 python3-pip"
    echo "  macOS: brew install python (or download from python.org)"
    echo "  CentOS/RHEL: sudo dnf install python3 python3-pip"
    exit 1
fi

echo "Python found:"
$PYTHON_CMD --version
echo

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo "ERROR: Python $PYTHON_VERSION found, but Python $REQUIRED_VERSION or higher is required"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    echo "ERROR: main.py not found in current directory"
    echo "Please run this script from the application folder"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "ERROR: requirements.txt not found"
    echo "Please make sure all application files are present"
    exit 1
fi

echo "Checking dependencies..."
echo

# Check if pip is available
if ! command_exists pip3 && ! command_exists pip; then
    echo "ERROR: pip is not installed"
    echo "Please install pip using your package manager"
    exit 1
fi

# Determine pip command
if command_exists pip3; then
    PIP_CMD="pip3"
else
    PIP_CMD="pip"
fi

# Try to import required modules
$PYTHON_CMD -c "import PyQt6" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "PyQt6 not found. Installing dependencies..."
    echo
    
    # Try to install dependencies
    $PIP_CMD install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to install dependencies with pip"
        echo "Trying alternative installation methods..."
        echo
        
        # Try with user flag
        echo "Trying user installation..."
        $PIP_CMD install --user -r requirements.txt
        if [ $? -ne 0 ]; then
            echo "ERROR: Failed to install dependencies"
            echo "Please try manually:"
            echo "  1. Update pip: $PIP_CMD install --upgrade pip"
            echo "  2. Install dependencies: $PIP_CMD install -r requirements.txt"
            echo "  3. Or use system packages (see SETUP_GUIDE.md)"
            exit 1
        fi
    fi
else
    echo "Dependencies appear to be installed."
fi

echo
echo "Starting DuckDB SQL GUI..."
echo
echo "If the application doesn't start, check the error messages above."
echo "Press Ctrl+C to stop the application."
echo

# Run the application
$PYTHON_CMD main.py

# Check exit code
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo
    echo "Application exited with error code: $EXIT_CODE"
    echo "Check the error messages above for troubleshooting."
    echo
    echo "Common solutions:"
    echo "1. Install missing dependencies: $PIP_CMD install -r requirements.txt"
    echo "2. Update Python to version 3.8 or higher"
    echo "3. Install system Qt6 packages (see SETUP_GUIDE.md)"
    echo "4. Check SETUP_GUIDE.md for detailed troubleshooting"
    echo
    read -p "Press Enter to continue..."
fi

echo
echo "Application closed."