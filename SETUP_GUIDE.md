# DuckDB SQL GUI - Setup Guide

This guide will help you set up and run the DuckDB SQL GUI application on any computer.

## System Requirements

- **Python**: 3.8 or higher (recommended: 3.9+)
- **Operating System**: Windows, macOS, or Linux
- **Memory**: At least 4GB RAM (8GB+ recommended for large datasets)

## Installation Steps

### 1. Install Python

If Python is not installed:
- **Windows**: Download from [python.org](https://www.python.org/downloads/) and check "Add Python to PATH"
- **macOS**: Use Homebrew: `brew install python` or download from python.org
- **Linux**: Use package manager: `sudo apt install python3 python3-pip` (Ubuntu/Debian)

### 2. Verify Python Installation

```bash
python --version
# or
python3 --version
```

Should show Python 3.8 or higher.

### 3. Create Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv duckdb_gui_env

# Activate it
# Windows:
duckdb_gui_env\Scripts\activate
# macOS/Linux:
source duckdb_gui_env/bin/activate
```

### 4. Install Dependencies

```bash
# Navigate to the application folder
cd path/to/sql_new

# Install required packages
pip install -r requirements.txt
```

### 5. Run the Application

```bash
python main.py
```

## Troubleshooting Common Issues

### Issue 1: "No module named 'PyQt6'"

**Solution:**
```bash
pip install PyQt6>=6.5.0
```

### Issue 2: "No module named 'duckdb'"

**Solution:**
```bash
pip install duckdb>=0.8.0
```

### Issue 3: "QScintilla not available"

**Solution:**
```bash
pip install QScintilla>=2.13.0
```

*Note: The application will work without QScintilla but with a basic text editor.*

### Issue 4: "Polars not available"

**Solution:**
```bash
pip install polars>=0.19.0
```

*Note: Folder loading features will be disabled without Polars.*

### Issue 5: Application doesn't start (no error message)

**Possible causes and solutions:**

1. **Missing display/GUI support (Linux):**
   ```bash
   # Install GUI libraries
   sudo apt-get install python3-pyqt6
   # or
   sudo apt-get install qt6-base-dev
   ```

2. **Permission issues:**
   ```bash
   # Make sure the script is executable
   chmod +x main.py
   ```

3. **Python path issues:**
   ```bash
   # Try using python3 explicitly
   python3 main.py
   ```

### Issue 6: "ImportError" or "ModuleNotFoundError"

**Solution:**
```bash
# Upgrade pip first
pip install --upgrade pip

# Reinstall all dependencies
pip uninstall -r requirements.txt -y
pip install -r requirements.txt
```

### Issue 7: Application crashes on startup

**Debug steps:**

1. **Run with verbose output:**
   ```bash
   python -v main.py
   ```

2. **Check Python compatibility:**
   ```bash
   python -c "import sys; print(sys.version)"
   ```

3. **Test individual imports:**
   ```bash
   python -c "import PyQt6; print('PyQt6 OK')"
   python -c "import duckdb; print('DuckDB OK')"
   python -c "import polars; print('Polars OK')"
   ```

## Platform-Specific Notes

### Windows
- Use Command Prompt or PowerShell
- If you get SSL errors, try: `pip install --trusted-host pypi.org --trusted-host pypi.python.org -r requirements.txt`

### macOS
- You might need to install Xcode Command Line Tools: `xcode-select --install`
- If using Apple Silicon (M1/M2), ensure you're using the correct Python architecture

### Linux
- Install system Qt6 libraries if pip installation fails:
  ```bash
  # Ubuntu/Debian
  sudo apt-get install python3-pyqt6 python3-pyqt6.qsci
  
  # CentOS/RHEL/Fedora
  sudo dnf install python3-qt6 python3-qscintilla-qt6
  ```

## Alternative Installation Methods

### Method 1: Using conda/mamba

```bash
# Create conda environment
conda create -n duckdb_gui python=3.9
conda activate duckdb_gui

# Install packages
conda install -c conda-forge pyqt duckdb polars pandas openpyxl xlsxwriter
pip install QScintilla
```

### Method 2: System-wide installation (not recommended)

```bash
# Install globally (use with caution)
sudo pip install -r requirements.txt
```

## Verification Steps

After installation, verify everything works:

1. **Start the application:**
   ```bash
   python main.py
   ```

2. **Check the console output:**
   - Should see: "DuckDB connection established with 'local' database"
   - No error messages

3. **Test basic functionality:**
   - Try running a simple query: `SELECT 1 as test`
   - Load a CSV file
   - Check if all tabs are accessible

## Getting Help

If you're still having issues:

1. **Check the console output** for specific error messages
2. **Verify all dependencies** are correctly installed
3. **Try running in a fresh virtual environment**
4. **Check Python and package versions** match requirements

## Performance Tips

- **Use SSD storage** for better file loading performance
- **Allocate sufficient RAM** for large datasets
- **Close other applications** if working with very large files
- **Use LIMIT clauses** when exploring large datasets initially

---

*Last updated: January 2025*