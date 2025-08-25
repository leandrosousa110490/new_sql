# DuckDB SQL GUI

A modern PyQt6-based database interface for DuckDB that allows you to load various file formats and execute SQL queries with a user-friendly graphical interface.

![DuckDB SQL GUI](screenshot.png)

## Features

- **Multi-format File Loading**: Support for CSV, Excel (.xlsx/.xls), JSON, and Parquet files
- **SQL Editor**: Syntax-highlighted SQL editor with auto-completion (when QScintilla is available)
- **Database Tree View**: Browse loaded tables and their column structures
- **Results Display**: Tabular display of query results with resizable columns
- **Error Handling**: Comprehensive error messages and logging
- **Threaded Execution**: Non-blocking query execution with progress indicators
- **Modern UI**: Clean, professional interface similar to popular database tools

## Requirements

- Python 3.8 or higher
- PyQt6
- DuckDB
- Polars (for Excel file support)
- QScintilla (optional, for enhanced SQL editor)

## Installation

1. **Clone or download this repository**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   python main.py
   ```

## Usage

### Loading Data Files

1. **Using the Menu**: Go to `File` menu and select the appropriate file type:
   - Load CSV File
   - Load Excel File
   - Load JSON File
   - Load Parquet File

2. **Using the Toolbar**: Click the corresponding button in the toolbar:
   - Load CSV
   - Load Excel
   - Load JSON
   - Load Parquet

3. **File Naming**: Files are automatically loaded as tables with names based on the filename (spaces and hyphens converted to underscores)

### Writing and Executing SQL Queries

1. **SQL Editor**: Write your SQL queries in the central editor panel
2. **Execute**: Press `F5` or click the "Execute (F5)" button
3. **View Results**: Results appear in the bottom panel's "Results" tab
4. **Check Messages**: Any errors or status messages appear in the "Messages" tab

### Database Tree

- **Tables**: Browse all loaded tables in the left panel
- **Columns**: Expand tables to see column names and data types
- **Auto-refresh**: Tree updates automatically when new tables are created

## Supported File Formats

### CSV Files
- Automatic delimiter detection
- Header row detection
- Data type inference

### Excel Files (.xlsx, .xls)
- Uses Polars for robust Excel reading
- Supports multiple sheets (loads first sheet by default)
- Handles various Excel formatting

### JSON Files
- Supports both regular JSON and JSON Lines (.jsonl)
- Automatic schema detection
- Nested object flattening

### Parquet Files
- Native DuckDB support
- Efficient columnar data loading
- Preserves data types and metadata

## Example Queries

```sql
-- View all loaded tables
SHOW TABLES;

-- Describe table structure
DESCRIBE my_table;

-- Basic data exploration
SELECT * FROM my_table LIMIT 10;

-- Aggregation example
SELECT 
    column1,
    COUNT(*) as count,
    AVG(column2) as avg_value
FROM my_table 
GROUP BY column1
ORDER BY count DESC;

-- Join multiple tables
SELECT 
    t1.id,
    t1.name,
    t2.value
FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id;
```

## Keyboard Shortcuts

- `F5`: Execute current query
- `Ctrl+N`: New query (clears editor)
- `Ctrl+O`: Open file dialog
- `Ctrl+Q`: Quit application

## Tips and Best Practices

1. **Large Files**: DuckDB handles large files efficiently, but consider using LIMIT for initial exploration
2. **Memory Usage**: The application uses an in-memory DuckDB instance by default
3. **File Paths**: Use forward slashes or double backslashes in file paths within SQL queries
4. **Data Types**: DuckDB automatically infers data types, but you can cast explicitly if needed
5. **Performance**: DuckDB is optimized for analytical queries and handles complex operations efficiently

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed via `pip install -r requirements.txt`
2. **File Loading Errors**: Check file permissions and format validity
3. **QScintilla Not Available**: The app will fall back to a basic text editor if QScintilla isn't installed
4. **Excel Files**: If Excel loading fails, ensure the file isn't password-protected or corrupted

### Error Messages

- Check the "Messages" tab for detailed error information
- Status bar shows connection status and query execution progress
- File loading errors are displayed in popup dialogs

## Technical Details

### Architecture

- **Main Window**: `DuckDBGUI` class manages the overall application
- **Database Tree**: `DatabaseTreeWidget` for browsing database objects
- **SQL Editor**: `SQLEditor` with optional syntax highlighting
- **Results Display**: `ResultsTableWidget` for query results
- **Query Execution**: `QueryWorker` thread for non-blocking execution

### Dependencies

- **PyQt6**: Modern GUI framework
- **DuckDB**: High-performance analytical database
- **Polars**: Fast DataFrame library for Excel support
- **QScintilla**: Advanced text editor with syntax highlighting (optional)

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve this application.

## License

This project is open source. Please check the license file for details.