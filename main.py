#!/usr/bin/env python3
"""
DuckDB SQL GUI Application
A PyQt6-based database interface for DuckDB with file loading capabilities
"""

import sys
import os
import traceback
from pathlib import Path
from typing import Optional, Dict, Any, List

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("Polars not available, folder loading feature will be disabled")

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTreeWidget, QTreeWidgetItem, QTextEdit, QTableWidget,
    QTableWidgetItem, QMenuBar, QToolBar, QStatusBar, QFileDialog,
    QMessageBox, QHeaderView, QAbstractItemView, QLabel, QPushButton,
    QProgressBar, QTabWidget, QMenu, QDialog, QFormLayout, QLineEdit,
    QCheckBox, QSpinBox, QDialogButtonBox, QComboBox, QGroupBox, QInputDialog,
    QCompleter
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QSettings, QStringListModel
from PyQt6.QtGui import QAction, QIcon, QFont, QPixmap, QClipboard

try:
    from PyQt6.Qsci import QsciScintilla, QsciLexerSQL
    QSCINTILLA_AVAILABLE = True
except ImportError:
    QSCINTILLA_AVAILABLE = False
    print("QScintilla not available, using basic text editor")

import duckdb
import polars as pl
import threading
import webbrowser
from flask import Flask, jsonify, request, send_from_directory
import json


class DatabaseConnection:
    """Represents a database connection configuration"""
    
    def __init__(self, name: str, db_type: str, host: str = '', port: int = 0, 
                 database: str = '', username: str = '', password: str = '', 
                 use_ssl: bool = False, ssl_cert: str = '', ssl_key: str = '', ssl_ca: str = ''):
        self.name = name
        self.db_type = db_type  # 'mariadb', 'mysql', 'postgresql', etc.
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.ssl_ca = ssl_ca
        self.connection = None
        self.is_connected = False


class DatabaseConnectionManager:
    """Manages multiple database connections"""
    
    def __init__(self, main_connection):
        self.main_connection = main_connection  # DuckDB main connection
        self.connections = {}  # Dict of connection_name -> DatabaseConnection
        self.settings = QSettings('DuckDBGUI', 'Connections')
        self.load_connections()
    
    def add_connection(self, connection: DatabaseConnection):
        """Add a new database connection"""
        self.connections[connection.name] = connection
        self.save_connections()
    
    def add_connection(self, name: str, host: str, port: int, database: str = '', 
                     username: str = '', password: str = '', ssl_cert: str = '', 
                     ssl_key: str = '', ssl_ca: str = '', db_type: str = 'mysql'):
        """Add a new database connection with individual parameters"""
        use_ssl = bool(ssl_cert or ssl_key or ssl_ca)
        connection = DatabaseConnection(
            name=name, db_type=db_type, host=host, port=port,
            database=database, username=username, password=password,
            use_ssl=use_ssl, ssl_cert=ssl_cert, ssl_key=ssl_key, ssl_ca=ssl_ca
        )
        self.connections[name] = connection
        self.save_connections()
    
    def remove_connection(self, name: str):
        """Remove a database connection"""
        if name in self.connections:
            conn = self.connections[name]
            if conn.is_connected:
                self.disconnect_database(name)
            del self.connections[name]
            self.save_connections()
    
    def connect_database(self, name: str) -> bool:
        """Connect to a database using DuckDB extensions"""
        if name not in self.connections:
            return False
            
        conn = self.connections[name]
        try:
            # Install and load the appropriate extension
            if conn.db_type in ['mysql', 'mariadb']:
                self.main_connection.execute("INSTALL mysql")
                self.main_connection.execute("LOAD mysql")
                
                # Build connection string using key-value format
                conn_str = f"host={conn.host} port={conn.port} user={conn.username}"
                # Only add database if specified
                if conn.database:
                    conn_str += f" database={conn.database}"
                if conn.password:
                    conn_str += f" password={conn.password}"
                
                if conn.use_ssl:
                    if conn.ssl_ca:
                        conn_str += f" sslca={conn.ssl_ca}"
                    if conn.ssl_cert:
                        conn_str += f" sslcert={conn.ssl_cert}"
                    if conn.ssl_key:
                        conn_str += f" sslkey={conn.ssl_key}"
                
                # Attach the database using TYPE mysql syntax
                self.main_connection.execute(f"ATTACH '{conn_str}' AS {conn.name} (TYPE mysql)")
                conn.is_connected = True
                return True
                
        except Exception as e:
            print(f"Failed to connect to {name}: {e}")
            return False
        
        return False
    
    def disconnect_database(self, name: str):
        """Disconnect from a database"""
        if name in self.connections:
            conn = self.connections[name]
            try:
                self.main_connection.execute(f"DETACH {name}")
                conn.is_connected = False
            except Exception as e:
                print(f"Error disconnecting {name}: {e}")
    
    def get_connection_names(self) -> List[str]:
        """Get list of all connection names"""
        return list(self.connections.keys())
    
    def get_connected_databases(self) -> List[str]:
        """Get list of connected database names"""
        return [name for name, conn in self.connections.items() if conn.is_connected]
    
    def save_connections(self):
        """Save connections to QSettings"""
        self.settings.beginWriteArray("connections")
        for i, (name, conn) in enumerate(self.connections.items()):
            self.settings.setArrayIndex(i)
            self.settings.setValue("name", conn.name)
            self.settings.setValue("db_type", conn.db_type)
            self.settings.setValue("host", conn.host)
            self.settings.setValue("port", conn.port)
            self.settings.setValue("database", conn.database)
            self.settings.setValue("username", conn.username)
            self.settings.setValue("password", conn.password)  # Save password
            self.settings.setValue("use_ssl", conn.use_ssl)
            self.settings.setValue("ssl_cert", conn.ssl_cert)
            self.settings.setValue("ssl_key", conn.ssl_key)
            self.settings.setValue("ssl_ca", conn.ssl_ca)
        self.settings.endArray()
    
    def load_connections(self):
        """Load connections from QSettings"""
        size = self.settings.beginReadArray("connections")
        for i in range(size):
            self.settings.setArrayIndex(i)
            name = self.settings.value("name", "")
            if name:
                conn = DatabaseConnection(
                    name=name,
                    db_type=self.settings.value("db_type", ""),
                    host=self.settings.value("host", ""),
                    port=int(self.settings.value("port", 0)),
                    database=self.settings.value("database", ""),
                    username=self.settings.value("username", ""),
                    password=self.settings.value("password", ""),  # Load password
                    use_ssl=self.settings.value("use_ssl", False, type=bool),
                    ssl_cert=self.settings.value("ssl_cert", ""),
                    ssl_key=self.settings.value("ssl_key", ""),
                    ssl_ca=self.settings.value("ssl_ca", "")
                )
                self.connections[name] = conn
        self.settings.endArray()


class CSVConfigDialog(QDialog):
    """Simplified dialog for configuring CSV automation settings"""
    
    def __init__(self, parent=None, current_config=None):
        super().__init__(parent)
        self.current_config = current_config or {}
        self.setWindowTitle("CSV Configuration")
        self.setModal(True)
        self.resize(400, 300)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Delimiter settings
        delimiter_group = QGroupBox("Delimiter Settings")
        delimiter_layout = QFormLayout(delimiter_group)
        
        self.delimiter_combo = QComboBox()
        self.delimiter_combo.addItems([
            "Comma (,)",
            "Semicolon (;)",
            "Tab",
            "Pipe (|)",
            "Custom"
        ])
        
        # Set current delimiter
        current_delim = self.current_config.get('delimiter', ',')
        if current_delim == ',':
            self.delimiter_combo.setCurrentIndex(0)
        elif current_delim == ';':
            self.delimiter_combo.setCurrentIndex(1)
        elif current_delim == '\t':
            self.delimiter_combo.setCurrentIndex(2)
        elif current_delim == '|':
            self.delimiter_combo.setCurrentIndex(3)
        else:
            self.delimiter_combo.setCurrentIndex(4)
            
        self.delimiter_combo.currentTextChanged.connect(self.on_delimiter_changed)
        delimiter_layout.addRow("Delimiter:", self.delimiter_combo)
        
        self.custom_delimiter_edit = QLineEdit()
        self.custom_delimiter_edit.setEnabled(current_delim not in [',', ';', '\t', '|'])
        self.custom_delimiter_edit.setText(current_delim if current_delim not in [',', ';', '\t', '|'] else '')
        delimiter_layout.addRow("Custom Delimiter:", self.custom_delimiter_edit)
        
        layout.addWidget(delimiter_group)
        
        # Additional options
        options_group = QGroupBox("Additional Options")
        options_layout = QFormLayout(options_group)
        
        self.header_check = QCheckBox("First row contains headers")
        self.header_check.setChecked(self.current_config.get('has_header', True))
        options_layout.addRow(self.header_check)
        
        self.quote_combo = QComboBox()
        self.quote_combo.addItems(["Auto", '"', "'", "None"])
        current_quote = self.current_config.get('quote_char', 'Auto')
        if current_quote in ['"', "'", "None"]:
            self.quote_combo.setCurrentText(current_quote)
        options_layout.addRow("Quote Character:", self.quote_combo)
        
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(["UTF-8", "UTF-16", "ISO-8859-1", "Windows-1252"])
        current_encoding = self.current_config.get('encoding', 'utf8')
        if current_encoding == 'utf8':
            self.encoding_combo.setCurrentText("UTF-8")
        elif current_encoding in ["UTF-16", "ISO-8859-1", "Windows-1252"]:
            self.encoding_combo.setCurrentText(current_encoding)
        options_layout.addRow("Encoding:", self.encoding_combo)
        
        layout.addWidget(options_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.accept)
        button_layout.addWidget(ok_button)
        
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(cancel_button)
        
        layout.addLayout(button_layout)
        
    def on_delimiter_changed(self, text):
        """Handle delimiter combo box changes"""
        is_custom = text == "Custom"
        self.custom_delimiter_edit.setEnabled(is_custom)
        if not is_custom:
            self.custom_delimiter_edit.clear()
            
    def get_config(self):
        """Get the current configuration as a dictionary"""
        delimiter_text = self.delimiter_combo.currentText()
        
        if delimiter_text == "Comma (,)":
            delimiter = ','
        elif delimiter_text == "Semicolon (;)":
            delimiter = ';'
        elif delimiter_text == "Tab":
            delimiter = '\t'
        elif delimiter_text == "Pipe (|)":
            delimiter = '|'
        else:  # Custom
            delimiter = self.custom_delimiter_edit.text() or ','
            
        quote_text = self.quote_combo.currentText()
        quote_char = None if quote_text in ["Auto", "None"] else quote_text
        
        encoding_text = self.encoding_combo.currentText()
        encoding_map = {
            "UTF-8": "utf8",
            "UTF-16": "utf16",
            "ISO-8859-1": "iso-8859-1",
            "Windows-1252": "windows-1252"
        }
        encoding = encoding_map.get(encoding_text, "utf8")
        
        return {
            'delimiter': delimiter,
            'has_header': self.header_check.isChecked(),
            'quote_char': quote_char,
            'encoding': encoding
        }


class CSVImportDialog(QDialog):
    """Dialog for configuring CSV import settings"""
    
    def __init__(self, parent=None, file_path: str = ""):
        super().__init__(parent)
        self.file_path = file_path
        self.setWindowTitle("CSV Import Settings")
        self.setModal(True)
        self.resize(500, 400)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # File info
        file_group = QGroupBox("File Information")
        file_layout = QFormLayout(file_group)
        
        self.file_label = QLabel(self.file_path)
        file_layout.addRow("File:", self.file_label)
        
        layout.addWidget(file_group)
        
        # Delimiter settings
        delimiter_group = QGroupBox("Delimiter Settings")
        delimiter_layout = QFormLayout(delimiter_group)
        
        self.delimiter_combo = QComboBox()
        self.delimiter_combo.addItems([
            "Automatic (recommended)",
            "Comma (,)",
            "Semicolon (;)",
            "Tab",
            "Pipe (|)",
            "Custom"
        ])
        self.delimiter_combo.setCurrentIndex(0)  # Default to automatic
        self.delimiter_combo.currentTextChanged.connect(self.on_delimiter_changed)
        delimiter_layout.addRow("Delimiter:", self.delimiter_combo)
        
        self.custom_delimiter_edit = QLineEdit()
        self.custom_delimiter_edit.setEnabled(False)
        self.custom_delimiter_edit.setPlaceholderText("Enter custom delimiter")
        delimiter_layout.addRow("Custom Delimiter:", self.custom_delimiter_edit)
        
        layout.addWidget(delimiter_group)
        
        # Additional options
        options_group = QGroupBox("Additional Options")
        options_layout = QFormLayout(options_group)
        
        self.header_check = QCheckBox("First row contains headers")
        self.header_check.setChecked(True)  # Default to true
        options_layout.addRow(self.header_check)
        
        self.quote_combo = QComboBox()
        self.quote_combo.addItems(["Auto", '"', "'", "None"])
        options_layout.addRow("Quote Character:", self.quote_combo)
        
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(["Auto", "UTF-8", "UTF-16", "ISO-8859-1", "Windows-1252"])
        options_layout.addRow("Encoding:", self.encoding_combo)
        
        layout.addWidget(options_group)
        
        # Preview section
        preview_group = QGroupBox("Preview (First 5 rows)")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_table = QTableWidget()
        self.preview_table.setMaximumHeight(150)
        preview_layout.addWidget(self.preview_table)
        
        self.preview_button = QPushButton("Update Preview")
        self.preview_button.clicked.connect(self.update_preview)
        preview_layout.addWidget(self.preview_button)
        
        layout.addWidget(preview_group)
        
        # Examples section
        examples_group = QGroupBox("Common Delimiter Examples")
        examples_layout = QVBoxLayout(examples_group)
        
        examples_text = QLabel(
            "• Comma (,): Standard CSV format - data1,data2,data3\n"
            "• Semicolon (;): European CSV format - data1;data2;data3\n"
            "• Tab: Tab-separated values - data1\tdata2\tdata3\n"
            "• Pipe (|): Alternative format - data1|data2|data3\n"
            "• Custom: Any other character you specify"
        )
        examples_text.setWordWrap(True)
        examples_text.setStyleSheet("color: #666; font-size: 9pt;")
        examples_layout.addWidget(examples_text)
        
        layout.addWidget(examples_group)
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        
        # Note: Preview is only updated when user clicks "Update Preview" button
        
    def on_delimiter_changed(self, text):
        """Enable/disable custom delimiter input based on selection"""
        is_custom = text == "Custom"
        self.custom_delimiter_edit.setEnabled(is_custom)
        # Note: Preview is only updated when user clicks "Update Preview" button
            
    def get_delimiter_value(self):
        """Get the actual delimiter value based on selection"""
        delimiter_text = self.delimiter_combo.currentText()
        
        if delimiter_text == "Automatic (recommended)":
            return None  # Use DuckDB's auto-detection
        elif delimiter_text == "Comma (,)":
            return ","
        elif delimiter_text == "Semicolon (;)":
            return ";"
        elif delimiter_text == "Tab":
            return "\t"
        elif delimiter_text == "Pipe (|)":
            return "|"
        elif delimiter_text == "Custom":
            return self.custom_delimiter_edit.text() or ","
        else:
            return ","  # Default fallback
            
    def get_quote_value(self):
        """Get the quote character value"""
        quote_text = self.quote_combo.currentText()
        if quote_text == "Auto":
            return None
        elif quote_text == "None":
            return ""
        else:
            return quote_text
            
    def update_preview(self):
        """Update the preview table with current settings"""
        try:
            import duckdb
            
            # Create temporary connection for preview
            temp_conn = duckdb.connect(":memory:")
            
            # Build the read_csv query based on settings
            delimiter = self.get_delimiter_value()
            quote_char = self.get_quote_value()
            
            if delimiter is None:
                # Use auto-detection
                query = f"SELECT * FROM read_csv_auto('{self.file_path}') LIMIT 5"
            else:
                # Use specific settings
                params = []
                params.append(f"delimiter='{delimiter}'")
                
                if not self.header_check.isChecked():
                    params.append("header=false")
                    
                if quote_char is not None:
                    if quote_char == "":
                        params.append("quote=''")
                    else:
                        params.append(f"quote='{quote_char}'")
                        
                params_str = ", " + ", ".join(params) if params else ""
                query = f"SELECT * FROM read_csv('{self.file_path}'{params_str}) LIMIT 5"
            
            result = temp_conn.execute(query).fetchall()
            columns = [desc[0] for desc in temp_conn.description]
            
            # Update preview table
            self.preview_table.setRowCount(len(result))
            self.preview_table.setColumnCount(len(columns))
            self.preview_table.setHorizontalHeaderLabels(columns)
            
            for row_idx, row_data in enumerate(result):
                for col_idx, cell_data in enumerate(row_data):
                    item = QTableWidgetItem(str(cell_data) if cell_data is not None else "")
                    self.preview_table.setItem(row_idx, col_idx, item)
                    
            self.preview_table.resizeColumnsToContents()
            temp_conn.close()
            
        except Exception as e:
            # Clear preview on error
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(1)
            self.preview_table.setHorizontalHeaderLabels(["Error"])
            error_item = QTableWidgetItem(f"Preview error: {str(e)}")
            self.preview_table.setRowCount(1)
            self.preview_table.setItem(0, 0, error_item)
            
    def get_csv_query(self, table_name: str):
        """Generate the appropriate CSV loading query based on settings"""
        delimiter = self.get_delimiter_value()
        quote_char = self.get_quote_value()
        
        if delimiter is None:
            # Use auto-detection
            return f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{self.file_path}')"
        else:
            # Use specific settings
            params = []
            params.append(f"delimiter='{delimiter}'")
            
            if not self.header_check.isChecked():
                params.append("header=false")
                
            if quote_char is not None:
                if quote_char == "":
                    params.append("quote=''")
                else:
                    params.append(f"quote='{quote_char}'")
                    
            params_str = ", " + ", ".join(params) if params else ""
            return f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv('{self.file_path}'{params_str})"
    
    def get_csv_query_as_text(self, table_name: str):
        """Generate CSV loading query with all columns as text (VARCHAR)"""
        delimiter = self.get_delimiter_value()
        quote_char = self.get_quote_value()
        
        if delimiter is None:
            # Use auto-detection with all columns as text
            return f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{self.file_path}', ALL_VARCHAR=true)"
        else:
            # Use specific settings with all columns as text
            params = []
            params.append(f"delimiter='{delimiter}'")
            params.append("ALL_VARCHAR=true")
            
            if not self.header_check.isChecked():
                params.append("header=false")
                
            if quote_char is not None:
                if quote_char == "":
                    params.append("quote=''")
                else:
                    params.append(f"quote='{quote_char}'")
                    
            params_str = ", " + ", ".join(params) if params else ""
            return f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv('{self.file_path}'{params_str})"


class ExcelImportDialog(QDialog):
    """Dialog for configuring Excel import settings"""
    
    def __init__(self, parent=None, file_path: str = ""):
        super().__init__(parent)
        self.file_path = file_path
        self.setWindowTitle("Excel Import Settings")
        self.setModal(True)
        self.resize(500, 400)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # File info
        file_group = QGroupBox("File Information")
        file_layout = QFormLayout(file_group)
        file_layout.addRow("File:", QLabel(self.file_path))
        layout.addWidget(file_group)
        
        # Sheet selection
        sheet_group = QGroupBox("Sheet Selection")
        sheet_layout = QFormLayout(sheet_group)
        
        self.sheet_input = QLineEdit()
        self.sheet_input.setPlaceholderText("Leave empty for first sheet")
        sheet_layout.addRow("Sheet Name:", self.sheet_input)
        
        # Add examples
        examples_label = QLabel(
            "Examples:\n"
            "• Leave empty: Uses the first sheet\n"
            "• 'Sheet1': Uses sheet named 'Sheet1'\n"
            "• 'Data': Uses sheet named 'Data'\n"
            "• 'Summary': Uses sheet named 'Summary'"
        )
        examples_label.setStyleSheet("color: #666; font-size: 11px; margin-top: 10px;")
        sheet_layout.addRow(examples_label)
        
        layout.addWidget(sheet_group)
        
        # Options
        options_group = QGroupBox("Import Options")
        options_layout = QFormLayout(options_group)
        
        self.header_checkbox = QCheckBox("First row contains headers")
        self.header_checkbox.setChecked(True)
        options_layout.addRow(self.header_checkbox)
        
        self.convert_to_text_checkbox = QCheckBox("Convert all columns to text")
        self.convert_to_text_checkbox.setChecked(False)
        self.convert_to_text_checkbox.setToolTip("Convert all data types to text to avoid type conflicts")
        options_layout.addRow(self.convert_to_text_checkbox)
        
        layout.addWidget(options_group)
        
        # Preview section
        preview_group = QGroupBox("Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_table = QTableWidget()
        self.preview_table.setMaximumHeight(150)
        preview_layout.addWidget(self.preview_table)
        
        self.preview_button = QPushButton("Update Preview")
        self.preview_button.clicked.connect(self.update_preview)
        preview_layout.addWidget(self.preview_button)
        
        layout.addWidget(preview_group)
        
        # Note: Preview is only updated when user clicks "Update Preview" button
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        
    def update_preview(self):
        """Update the preview table"""
        try:
            sheet_name = self.sheet_input.text().strip() or None
            
            # Read Excel file with specified sheet
            if sheet_name:
                df = pl.read_excel(self.file_path, sheet_name=sheet_name)
            else:
                df = pl.read_excel(self.file_path)  # First sheet by default
            
            # Limit preview to first 5 rows
            preview_df = df.head(5)
            
            # Setup table
            self.preview_table.setRowCount(min(5, len(preview_df)))
            self.preview_table.setColumnCount(len(preview_df.columns))
            
            # Set headers
            if self.header_checkbox.isChecked():
                self.preview_table.setHorizontalHeaderLabels([str(col) for col in preview_df.columns])
            else:
                self.preview_table.setHorizontalHeaderLabels([f"Column {i+1}" for i in range(len(preview_df.columns))])
            
            # Fill data
            for row in range(len(preview_df)):
                for col in range(len(preview_df.columns)):
                    value = preview_df.row(row)[col]
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    self.preview_table.setItem(row, col, item)
            
            # Resize columns to content
            self.preview_table.resizeColumnsToContents()
            
        except Exception as e:
            # Clear preview on error
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(1)
            self.preview_table.setHorizontalHeaderLabels(["Error"])
            error_item = QTableWidgetItem(f"Error reading sheet: {str(e)}")
            self.preview_table.setRowCount(1)
            self.preview_table.setItem(0, 0, error_item)
    
    def get_excel_query(self, table_name: str) -> str:
        """Generate the Excel loading query"""
        sheet_name = self.sheet_input.text().strip() or None
        
        if sheet_name:
            return f"CREATE TABLE local.{table_name} AS SELECT * FROM read_excel('{self.file_path}', sheet_name='{sheet_name}')"
        else:
            return f"CREATE TABLE local.{table_name} AS SELECT * FROM read_excel('{self.file_path}')"


class DatabaseConnectionDialog(QDialog):
    """Dialog for creating/editing database connections"""
    
    def __init__(self, parent=None, connection: DatabaseConnection = None):
        super().__init__(parent)
        self.connection = connection
        self.setup_ui()
        
        if connection:
            self.load_connection_data()
    
    def setup_ui(self):
        """Setup the connection dialog UI"""
        self.setWindowTitle("Database Connection")
        self.setModal(True)
        self.resize(500, 600)
        
        layout = QVBoxLayout(self)
        
        # Basic connection info
        basic_group = QGroupBox("Connection Details")
        basic_layout = QFormLayout(basic_group)
        
        self.name_edit = QLineEdit()
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["mysql", "mariadb", "postgresql"])
        self.host_edit = QLineEdit()
        self.host_edit.setText("localhost")  # Default host
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(3306)  # Default MySQL/MariaDB port
        self.database_edit = QLineEdit()
        self.username_edit = QLineEdit()
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        
        basic_layout.addRow("Connection Name:", self.name_edit)
        basic_layout.addRow("Database Type:", self.db_type_combo)
        basic_layout.addRow("Host:", self.host_edit)
        basic_layout.addRow("Port:", self.port_spin)
        basic_layout.addRow("Database:", self.database_edit)
        basic_layout.addRow("Username:", self.username_edit)
        basic_layout.addRow("Password:", self.password_edit)
        
        # SSL Configuration
        ssl_group = QGroupBox("SSL Configuration")
        ssl_layout = QFormLayout(ssl_group)
        
        self.use_ssl_check = QCheckBox("Use SSL")
        self.ssl_ca_edit = QLineEdit()
        self.ssl_cert_edit = QLineEdit()
        self.ssl_key_edit = QLineEdit()
        
        # SSL file browsers
        self.ssl_ca_btn = QPushButton("Browse...")
        self.ssl_cert_btn = QPushButton("Browse...")
        self.ssl_key_btn = QPushButton("Browse...")
        
        self.ssl_ca_btn.clicked.connect(lambda: self.browse_file(self.ssl_ca_edit, "CA Certificate"))
        self.ssl_cert_btn.clicked.connect(lambda: self.browse_file(self.ssl_cert_edit, "Client Certificate"))
        self.ssl_key_btn.clicked.connect(lambda: self.browse_file(self.ssl_key_edit, "Client Key"))
        
        ssl_layout.addRow(self.use_ssl_check)
        
        ca_layout = QHBoxLayout()
        ca_layout.addWidget(self.ssl_ca_edit)
        ca_layout.addWidget(self.ssl_ca_btn)
        ssl_layout.addRow("CA Certificate:", ca_layout)
        
        cert_layout = QHBoxLayout()
        cert_layout.addWidget(self.ssl_cert_edit)
        cert_layout.addWidget(self.ssl_cert_btn)
        ssl_layout.addRow("Client Certificate:", cert_layout)
        
        key_layout = QHBoxLayout()
        key_layout.addWidget(self.ssl_key_edit)
        key_layout.addWidget(self.ssl_key_btn)
        ssl_layout.addRow("Client Key:", key_layout)
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        
        # Test connection button
        test_btn = QPushButton("Test Connection")
        test_btn.clicked.connect(self.test_connection)
        button_box.addButton(test_btn, QDialogButtonBox.ButtonRole.ActionRole)
        
        layout.addWidget(basic_group)
        layout.addWidget(ssl_group)
        layout.addWidget(button_box)
        
        # Connect signals
        self.db_type_combo.currentTextChanged.connect(self.on_db_type_changed)
        
    def on_db_type_changed(self, db_type: str):
        """Update default port based on database type"""
        default_ports = {
            'mysql': 3306,
            'mariadb': 3306,
            'postgresql': 5432
        }
        self.port_spin.setValue(default_ports.get(db_type, 3306))
    
    def browse_file(self, line_edit: QLineEdit, title: str):
        """Browse for SSL certificate files"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, f"Select {title}", "", "Certificate Files (*.pem *.crt *.key);;All Files (*)"
        )
        if file_path:
            line_edit.setText(file_path)
    
    def load_connection_data(self):
        """Load existing connection data into the form"""
        if not self.connection:
            return
            
        self.name_edit.setText(self.connection.name)
        self.db_type_combo.setCurrentText(self.connection.db_type)
        self.host_edit.setText(self.connection.host)
        self.port_spin.setValue(self.connection.port)
        self.database_edit.setText(self.connection.database)
        self.username_edit.setText(self.connection.username)
        self.password_edit.setText(self.connection.password)
        self.use_ssl_check.setChecked(self.connection.use_ssl)
        self.ssl_ca_edit.setText(self.connection.ssl_ca)
        self.ssl_cert_edit.setText(self.connection.ssl_cert)
        self.ssl_key_edit.setText(self.connection.ssl_key)
    
    def get_connection_data(self) -> DatabaseConnection:
        """Get connection data from the form"""
        return DatabaseConnection(
            name=self.name_edit.text().strip(),
            db_type=self.db_type_combo.currentText(),
            host=self.host_edit.text().strip(),
            port=self.port_spin.value(),
            database=self.database_edit.text().strip(),
            username=self.username_edit.text().strip(),
            password=self.password_edit.text(),
            use_ssl=self.use_ssl_check.isChecked(),
            ssl_ca=self.ssl_ca_edit.text().strip(),
            ssl_cert=self.ssl_cert_edit.text().strip(),
            ssl_key=self.ssl_key_edit.text().strip()
        )
    
    def test_connection(self):
        """Test the database connection"""
        conn_data = self.get_connection_data()
        
        # Database field is optional - only name, host, and username are required
        if not all([conn_data.name, conn_data.host, conn_data.username]):
            QMessageBox.warning(self, "Validation Error", "Please fill in all required fields (Name, Host, Username).")
            return
        
        # Test the actual connection
        try:
            import duckdb
            
            # Create a temporary connection for testing
            test_conn = duckdb.connect(':memory:')
            
            # Install and load MySQL extension
            try:
                test_conn.execute("INSTALL mysql")
                test_conn.execute("LOAD mysql")
            except Exception as e:
                print(f"MySQL extension setup: {e}")
            
            # Build connection string
            conn_str = f"host={conn_data.host} port={conn_data.port} user={conn_data.username}"
            # Only add database if specified
            if conn_data.database:
                conn_str += f" database={conn_data.database}"
            if conn_data.password:
                conn_str += f" password={conn_data.password}"
            
            if conn_data.use_ssl:
                if conn_data.ssl_ca:
                    conn_str += f" sslca={conn_data.ssl_ca}"
                if conn_data.ssl_cert:
                    conn_str += f" sslcert={conn_data.ssl_cert}"
                if conn_data.ssl_key:
                    conn_str += f" sslkey={conn_data.ssl_key}"
            
            # Test the connection by trying to attach
            test_db_name = f"test_{conn_data.name}"
            test_conn.execute(f"ATTACH '{conn_str}' AS {test_db_name} (TYPE mysql)")
            
            # Try a simple query to verify connection
            # Use a basic query that works regardless of database specification
            result = test_conn.execute(f"SELECT 1").fetchone()
            
            # Clean up
            test_conn.execute(f"DETACH {test_db_name}")
            test_conn.close()
            
            database_info = conn_data.database if conn_data.database else "All databases"
            QMessageBox.information(self, "Connection Test", 
                                  f"✅ Connection successful!\n\n"
                                  f"Host: {conn_data.host}:{conn_data.port}\n"
                                  f"Database: {database_info}\n"
                                  f"User: {conn_data.username}\n"
                                  f"SSL: {'Yes' if conn_data.use_ssl else 'No'}")
            
        except Exception as e:
            error_msg = str(e)
            if "Access denied" in error_msg:
                error_msg = "Access denied. Please check your username and password."
            elif "Can't connect" in error_msg or "Connection refused" in error_msg:
                error_msg = f"Cannot connect to {conn_data.host}:{conn_data.port}. Please check if the server is running and accessible."
            elif "Unknown database" in error_msg:
                error_msg = f"Database '{conn_data.database}' does not exist on the server."
            
            QMessageBox.critical(self, "Connection Test Failed", 
                               f"❌ Connection failed:\n\n{error_msg}\n\n"
                               f"Host: {conn_data.host}:{conn_data.port}\n"
                               f"Database: {conn_data.database}\n"
                               f"User: {conn_data.username}")
    
    def accept(self):
        """Validate and accept the dialog"""
        conn_data = self.get_connection_data()
        
        # Database field is optional - only name, host, and username are required
        if not all([conn_data.name, conn_data.host, conn_data.username]):
            QMessageBox.warning(self, "Validation Error", "Please fill in all required fields (Name, Host, Username).")
            return
        
        super().accept()


class ThemeManager:
    """Manages application themes and styling"""
    
    def __init__(self):
        self.settings = QSettings('DuckDBGUI', 'Themes')
        self.current_theme = self.settings.value('current_theme', 'light')
        
    def get_themes(self):
        """Get available theme names"""
        return ['light', 'dark', 'blue', 'green']
        
    def get_theme_stylesheet(self, theme_name):
        """Get stylesheet for a specific theme"""
        themes = {
            'light': self._get_light_theme(),
            'dark': self._get_dark_theme(),
            'blue': self._get_blue_theme(),
            'green': self._get_green_theme()
        }
        return themes.get(theme_name, themes['light'])
        
    def _get_light_theme(self):
        """Light theme stylesheet"""
        return """
        QMainWindow {
            background-color: #f0f0f0;
            color: #333333;
        }
        QTreeWidget {
            background-color: #ffffff;
            color: #333333;
            border: 1px solid #cccccc;
            selection-background-color: #e3f2fd;
        }
        QTreeWidget::item:hover {
            background-color: #f5f5f5;
        }
        QTextEdit, QPlainTextEdit {
            background-color: #ffffff;
            color: #333333;
            border: 1px solid #cccccc;
            font-family: 'Consolas', 'Monaco', monospace;
        }
        QTableWidget {
            background-color: #ffffff;
            color: #333333;
            gridline-color: #e0e0e0;
            selection-background-color: #e3f2fd;
        }
        QMenuBar {
            background-color: #f8f8f8;
            color: #333333;
            border-bottom: 1px solid #cccccc;
        }
        QMenuBar::item:selected {
            background-color: #e0e0e0;
        }
        QToolBar {
            background-color: #f8f8f8;
            border: 1px solid #cccccc;
        }
        QPushButton {
            background-color: #ffffff;
            color: #333333;
            border: 1px solid #cccccc;
            padding: 5px 10px;
            border-radius: 3px;
        }
        QPushButton:hover {
            background-color: #f0f0f0;
        }
        QPushButton:pressed {
            background-color: #e0e0e0;
        }
        QStatusBar {
            background-color: #f8f8f8;
            color: #333333;
            border-top: 1px solid #cccccc;
        }
        QStatusBar QLabel {
            color: #333333;
        }
        """
        
    def _get_dark_theme(self):
        """Dark theme stylesheet"""
        return """
        QMainWindow {
            background-color: #2b2b2b;
            color: #ffffff;
        }
        QTreeWidget {
            background-color: #3c3c3c;
            color: #ffffff;
            border: 1px solid #555555;
            selection-background-color: #4a4a4a;
        }
        QTreeWidget::item:hover {
            background-color: #404040;
        }
        QTextEdit, QPlainTextEdit {
            background-color: #2d2d2d;
            color: #ffffff;
            border: 1px solid #555555;
            font-family: 'Consolas', 'Monaco', monospace;
        }
        QTableWidget {
            background-color: #3c3c3c;
            color: #ffffff;
            gridline-color: #555555;
            selection-background-color: #4a4a4a;
        }
        QMenuBar {
            background-color: #2b2b2b;
            color: #ffffff;
            border-bottom: 1px solid #555555;
        }
        QMenuBar::item:selected {
            background-color: #404040;
        }
        QToolBar {
            background-color: #2b2b2b;
            border: 1px solid #555555;
        }
        QPushButton {
            background-color: #3c3c3c;
            color: #ffffff;
            border: 1px solid #555555;
            padding: 5px 10px;
            border-radius: 3px;
        }
        QPushButton:hover {
            background-color: #4a4a4a;
        }
        QPushButton:pressed {
            background-color: #555555;
        }
        QTabWidget::pane {
            background-color: #3c3c3c;
            border: 1px solid #555555;
        }
        QTabBar::tab {
            background-color: #2b2b2b;
            color: #ffffff;
            padding: 5px 10px;
            border: 1px solid #555555;
        }
        QTabBar::tab:selected {
            background-color: #3c3c3c;
        }
        QStatusBar {
            background-color: #2b2b2b;
            color: #ffffff;
            border-top: 1px solid #555555;
        }
        QStatusBar QLabel {
            color: #ffffff;
        }
        """
        
    def _get_blue_theme(self):
        """Blue theme stylesheet"""
        return """
        QMainWindow {
            background-color: #e3f2fd;
            color: #0d47a1;
        }
        QTreeWidget {
            background-color: #ffffff;
            color: #0d47a1;
            border: 1px solid #2196f3;
            selection-background-color: #bbdefb;
        }
        QTreeWidget::item:hover {
            background-color: #f3f9ff;
        }
        QTextEdit, QPlainTextEdit {
            background-color: #ffffff;
            color: #0d47a1;
            border: 1px solid #2196f3;
            font-family: 'Consolas', 'Monaco', monospace;
        }
        QTableWidget {
            background-color: #ffffff;
            color: #0d47a1;
            gridline-color: #90caf9;
            selection-background-color: #bbdefb;
        }
        QMenuBar {
            background-color: #1976d2;
            color: #ffffff;
        }
        QMenuBar::item:selected {
            background-color: #1565c0;
        }
        QToolBar {
            background-color: #1976d2;
            border: 1px solid #1565c0;
        }
        QPushButton {
            background-color: #2196f3;
            color: #ffffff;
            border: 1px solid #1976d2;
            padding: 5px 10px;
            border-radius: 3px;
        }
        QPushButton:hover {
            background-color: #1976d2;
        }
        QPushButton:pressed {
            background-color: #1565c0;
        }
        QStatusBar {
            background-color: #e3f2fd;
            color: #0d47a1;
            border-top: 1px solid #2196f3;
        }
        QStatusBar QLabel {
            color: #0d47a1;
        }
        """
        
    def _get_green_theme(self):
        """Green theme stylesheet"""
        return """
        QMainWindow {
            background-color: #e8f5e8;
            color: #1b5e20;
        }
        QTreeWidget {
            background-color: #ffffff;
            color: #1b5e20;
            border: 1px solid #4caf50;
            selection-background-color: #c8e6c9;
        }
        QTreeWidget::item:hover {
            background-color: #f1f8e9;
        }
        QTextEdit, QPlainTextEdit {
            background-color: #ffffff;
            color: #1b5e20;
            border: 1px solid #4caf50;
            font-family: 'Consolas', 'Monaco', monospace;
        }
        QTableWidget {
            background-color: #ffffff;
            color: #1b5e20;
            gridline-color: #a5d6a7;
            selection-background-color: #c8e6c9;
        }
        QMenuBar {
            background-color: #388e3c;
            color: #ffffff;
        }
        QMenuBar::item:selected {
            background-color: #2e7d32;
        }
        QToolBar {
            background-color: #388e3c;
            border: 1px solid #2e7d32;
        }
        QPushButton {
            background-color: #4caf50;
            color: #ffffff;
            border: 1px solid #388e3c;
            padding: 5px 10px;
            border-radius: 3px;
        }
        QPushButton:hover {
            background-color: #388e3c;
        }
        QPushButton:pressed {
            background-color: #2e7d32;
        }
        QStatusBar {
            background-color: #e8f5e8;
            color: #1b5e20;
            border-top: 1px solid #4caf50;
        }
        QStatusBar QLabel {
            color: #1b5e20;
        }
        """
        
    def set_theme(self, theme_name):
        """Set the current theme"""
        self.current_theme = theme_name
        self.settings.setValue('current_theme', theme_name)
        
    def get_current_theme(self):
        """Get the current theme name"""
        return self.current_theme


class QueryWorker(QThread):
    """Worker thread for executing SQL queries with pagination support"""
    finished = pyqtSignal(object, str, int)  # Results, query, total_count
    error = pyqtSignal(str)
    progress = pyqtSignal(str)
    
    def __init__(self, connection, query, current_database=None, current_connection=None, page_size=1000, page_number=0):
        super().__init__()
        self.connection = connection
        self.query = query
        self.current_database = current_database
        self.current_connection = current_connection
        self.page_size = page_size
        self.page_number = page_number
        
    def preprocess_query(self, query):
        """Preprocess query to handle database context"""
        query = query.strip()
        
        # Handle USE database statements
        if query.upper().startswith('USE '):
            # Extract just the database name, stopping at semicolon or newline
            use_part = query[4:].strip()
            # Find the end of the USE statement (semicolon or newline)
            end_pos = len(use_part)
            for i, char in enumerate(use_part):
                if char in [';', '\n', '\r']:
                    end_pos = i
                    break
            db_name = use_part[:end_pos].strip()
            
            # Update the current database context
            self.current_database = db_name
            # For DuckDB with attached databases, we can't use USE directly
            # Instead, we'll return a special signal to update the current database context
            raise Exception(f"Database context switched to '{db_name}'. Please run your next query.")
        
        # If we have a current database context, try to prefix unqualified table references
        if self.current_database and self.current_database != 'local':
            # This is a simple approach - for more complex queries, a proper SQL parser would be needed
            # Handle common patterns like "FROM table" or "JOIN table"
            import re
            
            # Build the proper prefix: connection.database for attached databases
            table_prefix = self.current_database
            if self.current_connection and self.current_connection != 'local':
                table_prefix = f"{self.current_connection}.{self.current_database}"
            
            # Pattern to match table references that might need connection prefixing
            # Handle both single table names and database.table format
            patterns = [
                (r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]+)(\s|$|;|,|\))', 'FROM'),
                (r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]+)(\s|$|;|,|\))', 'JOIN'),
                (r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_.]+)(\s|$|;|,|\))', 'INTO'),
                (r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_.]+)(\s|$|;|,|\))', 'UPDATE'),
            ]
            
            for pattern, keyword in patterns:
                matches = re.finditer(pattern, query, re.IGNORECASE)
                for match in reversed(list(matches)):
                    table_ref = match.group(1)
                    suffix = match.group(2)
                    
                    # Determine the correct table reference
                    if '.' not in table_ref:
                        # Single table name - add appropriate prefix
                        if self.current_connection != 'local':
                            new_table_ref = f"{table_prefix}.{table_ref}"
                        else:
                            # For local connection, just use the table name
                            new_table_ref = table_ref
                    elif table_ref.count('.') == 1:
                        # Handle database.table or local.table format
                        parts = table_ref.split('.')
                        db_part, table_part = parts[0], parts[1]
                        
                        if db_part == 'local':
                            # local.table - keep as is for local tables
                            new_table_ref = table_ref
                        elif self.current_connection != 'local':
                            # database.table format for attached databases
                            if not table_ref.startswith(self.current_connection + '.'):
                                new_table_ref = f"{self.current_connection}.{table_ref}"
                            else:
                                new_table_ref = table_ref
                        else:
                            # We're in local context but referencing database.table
                            # This might be a reference to an attached database table
                            new_table_ref = table_ref
                    else:
                        # Already fully qualified (connection.database.table) - leave as is
                        new_table_ref = table_ref
                    
                    # Replace the match
                    replacement = f"{keyword} {new_table_ref}{suffix}"
                    query = query[:match.start()] + replacement + query[match.end():]
        
        return query
        
    def run(self):
        try:
            self.progress.emit("Executing query...")
            
            # Preprocess the query to handle database context
            processed_query = self.preprocess_query(self.query)
            
            # Check if query is a SELECT statement that can be paginated
            query_upper = processed_query.strip().upper()
            is_select = query_upper.startswith('SELECT') or query_upper.startswith('WITH')
            
            if is_select and self.page_size > 0:
                # Get total count first
                self.progress.emit("Counting total rows...")
                # Clean the query by removing trailing semicolons and whitespace
                clean_query_for_count = processed_query.rstrip().rstrip(';')
                count_query = f"SELECT COUNT(*) FROM ({clean_query_for_count}) AS count_subquery"
                try:
                    total_count = self.connection.execute(count_query).fetchone()[0]
                except:
                    # If count fails, fall back to non-paginated query
                    total_count = -1
                    result = self.connection.execute(processed_query).fetchall()
                    columns = [desc[0] for desc in self.connection.description]
                    self.finished.emit((result, columns), self.query, total_count)
                    return
                
                # Execute paginated query
                self.progress.emit(f"Loading page {self.page_number + 1}...")
                offset = self.page_number * self.page_size
                
                # Clean the query by removing trailing semicolons and whitespace
                clean_query = processed_query.rstrip().rstrip(';')
                
                # Check if query already has LIMIT clause
                query_upper = clean_query.upper()
                if 'LIMIT' in query_upper:
                    # If query already has LIMIT, wrap it in a subquery for pagination
                    paginated_query = f"SELECT * FROM ({clean_query}) AS paginated_subquery LIMIT {self.page_size} OFFSET {offset}"
                else:
                    # Add LIMIT and OFFSET to the original query
                    paginated_query = f"{clean_query} LIMIT {self.page_size} OFFSET {offset}"
                
                result = self.connection.execute(paginated_query).fetchall()
                columns = [desc[0] for desc in self.connection.description]
                self.finished.emit((result, columns), self.query, total_count)
            else:
                # Non-SELECT queries or when pagination is disabled
                result = self.connection.execute(processed_query).fetchall()
                columns = [desc[0] for desc in self.connection.description]
                total_count = len(result) if result else 0
                self.finished.emit((result, columns), self.query, total_count)
                
        except Exception as e:
            self.error.emit(str(e))


class DatabaseTreeWidget(QTreeWidget):
    """Custom tree widget for database objects"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent_gui = parent
        self.setHeaderLabel("Database Objects")
        self.setMinimumWidth(250)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
        self.setup_tree()
        
    def setup_tree(self):
        """Initialize the database tree structure"""
        # Create local source node
        self.local_source_node = QTreeWidgetItem(self, ["Local DuckDB"])
        self.local_db_node = QTreeWidgetItem(self.local_source_node, ["local"])
        self.local_tables_node = QTreeWidgetItem(self.local_db_node, ["Tables"])
        self.local_views_node = QTreeWidgetItem(self.local_db_node, ["Views"])
        
        # Dictionary to store nodes
        self.source_nodes = {'local': self.local_source_node}
        self.database_nodes = {'local': self.local_db_node}
        self.table_nodes = {'local': self.local_tables_node}
        self.view_nodes = {'local': self.local_views_node}
        
        # Keep all nodes collapsed by default
        self.collapseAll()
        
    def add_database(self, db_name: str):
        """Add a database to the tree"""
        if db_name not in self.database_nodes:
            # Create source node if it doesn't exist
            if db_name not in self.source_nodes:
                source_item = QTreeWidgetItem(self, [f"{db_name} Connection"])
                self.source_nodes[db_name] = source_item
            
            # Create database node under source
            db_item = QTreeWidgetItem(self.source_nodes[db_name], [db_name])
            tables_item = QTreeWidgetItem(db_item, ["Tables"])
            views_item = QTreeWidgetItem(db_item, ["Views"])
            
            self.database_nodes[db_name] = db_item
            self.table_nodes[db_name] = tables_item
            self.view_nodes[db_name] = views_item
            
            # Keep collapsed by default
            self.collapseAll()
        
    def add_table(self, table_name: str, columns: List[str] = None, database: str = 'local'):
        """Add a table to the tree"""
        if database not in self.table_nodes:
            self.add_database(database)
            
        table_item = QTreeWidgetItem(self.table_nodes[database], [table_name])
        table_item.setData(0, Qt.ItemDataRole.UserRole, database)  # Store database name
        
        if columns:
            for col in columns:
                col_item = QTreeWidgetItem(table_item, [col])
                table_item.addChild(col_item)
        
        self.table_nodes[database].addChild(table_item)
        # Keep collapsed by default - don't expand
        
    def remove_database(self, db_name: str):
        """Remove a database from the tree"""
        if db_name in self.database_nodes and db_name != 'local':
            # Remove the source node (which contains the database)
            if db_name in self.source_nodes:
                self.takeTopLevelItem(self.indexOfTopLevelItem(self.source_nodes[db_name]))
                del self.source_nodes[db_name]
            
            # Clean up all related nodes
            if db_name in self.database_nodes:
                del self.database_nodes[db_name]
            if db_name in self.table_nodes:
                del self.table_nodes[db_name]
            if db_name in self.view_nodes:
                del self.view_nodes[db_name]
        
    def show_context_menu(self, position):
        """Show context menu for tree items"""
        item = self.itemAt(position)
        if not item:
            return
            
        # Check if it's a table item (child of tables_node)
        if item.parent() and item.parent().text(0) == "Tables":
            menu = QMenu(self)
            database = item.data(0, Qt.ItemDataRole.UserRole) or 'local'
            
            # Add common actions
            select_action = menu.addAction("Select * FROM table")
            describe_action = menu.addAction("Describe table")
            
            # Initialize action variables
            rename_action = None
            remove_action = None
            delete_action = None
            
            # Add rename and remove options only for local tables
            if database == 'local':
                menu.addSeparator()
                rename_action = menu.addAction("Rename table")
                remove_action = menu.addAction("Remove table")
            
            # Execute menu
            action = menu.exec(self.mapToGlobal(position))
            
            if action == select_action:
                self.select_from_table(item.text(0), database)
            elif action == describe_action:
                self.describe_table(item.text(0), database)
            elif database == 'local' and rename_action and action == rename_action:
                self.rename_table(item.text(0), database)
            elif database == 'local' and remove_action and action == remove_action:
                self.remove_table(item.text(0), database)

                
    def get_selected_table_info(self):
        """Get table name and database from selected item"""
        current_item = self.currentItem()
        if current_item and current_item.parent():
            parent = current_item.parent()
            if parent.text(0) == "Tables":
                table_name = current_item.text(0)
                database = current_item.data(0, Qt.ItemDataRole.UserRole) or 'local'
                return table_name, database
        return None, None
    
    def select_from_table(self, table_name: str, database: str = None):
        """Insert SELECT query for table"""
        if self.parent_gui:
            database = database or 'local'
            if database == 'local':
                query = f"SELECT * FROM local.{table_name} LIMIT 100;"
            else:
                query = f"SELECT * FROM {database}.{table_name} LIMIT 100;"
            current_editor = self.parent_gui.get_current_editor()
            if current_editor:
                current_editor.set_text(query)
            
    def describe_table(self, table_name: str, database: str = None):
        """Insert DESCRIBE query for table"""
        if self.parent_gui:
            database = database or 'local'
            if database == 'local':
                query = f"DESCRIBE local.{table_name};"
            else:
                query = f"DESCRIBE {database}.{table_name};"
            current_editor = self.parent_gui.get_current_editor()
            if current_editor:
                current_editor.set_text(query)
            
    def delete_table(self, table_name: str, database: str = None):
        """Delete table after confirmation"""
        if self.parent_gui:
            database = database or 'local'
            reply = QMessageBox.question(
                self, 
                "Delete Table",
                f"Are you sure you want to delete table '{table_name}' from {database}?\n\nThis action cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    if database == 'local':
                        self.parent_gui.connection.execute(f"DROP TABLE local.{table_name}")
                        self.parent_gui.log_message(f"Table 'local.{table_name}' deleted successfully")
                    else:
                        self.parent_gui.connection.execute(f"DROP TABLE {database}.{table_name}")
                        self.parent_gui.log_message(f"Table '{database}.{table_name}' deleted successfully")
                    self.parent_gui.refresh_database_tree()
                except Exception as e:
                    error_msg = f"Error deleting table '{database}.{table_name}': {str(e)}"
                    self.parent_gui.log_message(error_msg)
                    QMessageBox.critical(self, "Delete Error", error_msg)
    
    def rename_table(self, table_name: str, database: str = None):
        """Rename a local table"""
        if self.parent_gui and database == 'local':
            new_name, ok = QInputDialog.getText(
                self, 
                "Rename Table", 
                f"Enter new name for table '{table_name}':",
                text=table_name
            )
            
            if ok and new_name and new_name != table_name:
                try:
                    # Check if new name already exists
                    result = self.parent_gui.connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                        [new_name]
                    ).fetchone()
                    
                    if result:
                        QMessageBox.warning(
                            self, 
                            "Rename Error", 
                            f"Table '{new_name}' already exists. Please choose a different name."
                        )
                        return
                    
                    # Rename the table
                    self.parent_gui.connection.execute(f"ALTER TABLE local.{table_name} RENAME TO {new_name}")
                    self.parent_gui.log_message(f"Table 'local.{table_name}' renamed to 'local.{new_name}' successfully")
                    self.parent_gui.refresh_database_tree()
                    
                except Exception as e:
                    error_msg = f"Error renaming table '{table_name}': {str(e)}"
                    self.parent_gui.log_message(error_msg)
                    QMessageBox.critical(self, "Rename Error", error_msg)
    
    def remove_table(self, table_name: str, database: str = None):
        """Remove a local table (same as delete but with different confirmation message)"""
        if self.parent_gui and database == 'local':
            reply = QMessageBox.question(
                self, 
                "Remove Table",
                f"Are you sure you want to remove table '{table_name}' from the local database?\n\nThis action cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    self.parent_gui.connection.execute(f"DROP TABLE local.{table_name}")
                    self.parent_gui.log_message(f"Table 'local.{table_name}' removed successfully")
                    self.parent_gui.refresh_database_tree()
                except Exception as e:
                    error_msg = f"Error removing table '{table_name}': {str(e)}"
                    self.parent_gui.log_message(error_msg)
                    QMessageBox.critical(self, "Remove Error", error_msg)


class CustomSQLLexer(QsciLexerSQL):
    """Custom SQL Lexer with additional keywords including USE"""
    
    def __init__(self, parent=None, highlighted_keywords=None, non_highlighted_keywords=None):
        super().__init__(parent)
        self.highlighted_keywords = highlighted_keywords or []
        self.non_highlighted_keywords = non_highlighted_keywords or []
    
    def keywords(self, set):
        """Override keywords method to include custom keywords and exclude non-highlighted ones"""
        # Get the original keywords from the parent class
        original_keywords = super().keywords(set)
        
        if set == 1:  # Primary keyword set
            # Start with original keywords
            all_keywords = []
            if original_keywords:
                all_keywords.extend(original_keywords.split())
            
            # Add custom highlighted keywords
            for keyword in self.highlighted_keywords:
                all_keywords.append(keyword.upper())
                all_keywords.append(keyword.lower())
            
            # Remove non-highlighted keywords
            non_highlighted_list = []
            for keyword in self.non_highlighted_keywords:
                non_highlighted_list.append(keyword.upper())
                non_highlighted_list.append(keyword.lower())
            non_highlighted_set = frozenset(non_highlighted_list)
            
            # Filter out non-highlighted keywords
            filtered_keywords = [kw for kw in all_keywords if kw not in non_highlighted_set]
            
            return " ".join(filtered_keywords) if filtered_keywords else ""
        
        return original_keywords


class SQLEditor(QWidget):
    """SQL Editor with syntax highlighting"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_theme = 'light'
        self.parent_gui = parent
        self.completer = None
        self.table_names = []
        
        # Define SQL keywords that should be highlighted in blue
        self.sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE',
            'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON',
            'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
            'UNION', 'INTERSECT', 'EXCEPT', 'AS', 'DISTINCT',
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'AND', 'OR', 'NOT',
            'USE', 'DATABASE', 'SCHEMA', 'SHOW', 'DESCRIBE', 'DESC',
            'IF', 'EXISTS', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES',
            'UNIQUE', 'NULL', 'DEFAULT', 'AUTO_INCREMENT', 'CONSTRAINT',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'BETWEEN', 'IN',
            'LIKE', 'IS', 'TRUE', 'FALSE', 'SHOW', 'DATABASES', 'TABLES'
        ]
        
        # Define SQL commands that should NOT be highlighted in blue
        # You can manually add commands here that should appear as regular text
        self.non_highlighted_keywords = [
            # Add your custom SQL commands here that should not be highlighted
            # Example: 'CUSTOM_COMMAND', 'SPECIAL_FUNCTION'
        ]
        
        self.setup_ui()
        self.setup_completer()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        if QSCINTILLA_AVAILABLE:
            self.editor = QsciScintilla()
            self.lexer = CustomSQLLexer(self, self.sql_keywords, self.non_highlighted_keywords)
            self.editor.setLexer(self.lexer)
            self.editor.setAutoIndent(True)
            self.editor.setIndentationsUseTabs(False)
            self.editor.setIndentationWidth(4)
            self.editor.setTabWidth(4)
            # Autocompletion will be configured in update_table_names method
            
            # Set font
            font = QFont('Consolas', 10)
            self.editor.setFont(font)
            self.lexer.setFont(font)
        else:
            self.editor = QTextEdit()
            font = QFont('Consolas', 10)
            self.editor.setFont(font)
            
        layout.addWidget(self.editor)
        
    def setup_completer(self):
        """Setup autocomplete functionality"""
        if not QSCINTILLA_AVAILABLE:
            # Only setup QCompleter for QTextEdit
            self.completer = QCompleter()
            self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
            self.completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
            
            # Create string list model for table names
            self.model = QStringListModel()
            self.completer.setModel(self.model)
            self.editor.setCompleter(self.completer)
        else:
            # For QsciScintilla, we'll setup autocompletion in update_table_names
            self.completer = None
            self.model = None
    
    def update_table_names(self, table_names: list):
        """Update the list of table names for autocomplete"""
        print(f"DEBUG SQLEditor: update_table_names called with {len(table_names)} tables: {table_names[:5]}...")  # Print to console for immediate visibility
        self.table_names = table_names
        
        if QSCINTILLA_AVAILABLE and hasattr(self, 'editor') and hasattr(self, 'lexer'):
            # For QsciScintilla, use the built-in autocompletion
            from PyQt6.Qsci import QsciAPIs
            
            # Clear existing API if it exists
            if hasattr(self, 'api'):
                self.api.clear()
            
            # Create API for autocompletion
            self.api = QsciAPIs(self.lexer)
            
            # Add SQL keywords (both highlighted and non-highlighted for autocompletion)
            all_keywords = self.sql_keywords + self.non_highlighted_keywords
            
            for keyword in all_keywords:
                self.api.add(keyword)  # Add uppercase version
                self.api.add(keyword.lower())  # Add lowercase version
            
            # Add table names
            for table_name in table_names:
                self.api.add(table_name)  # Add original case only
            
            # Prepare the API
            self.api.prepare()
            
            # Enable autocompletion
            self.editor.setAutoCompletionSource(QsciScintilla.AutoCompletionSource.AcsAPIs)
            self.editor.setAutoCompletionThreshold(1)
            self.editor.setAutoCompletionCaseSensitivity(False)
            self.editor.setAutoCompletionReplaceWord(True)
            self.editor.setAutoCompletionShowSingle(True)
            
            print(f"DEBUG SQLEditor: QsciScintilla autocomplete updated with {len(table_names)} table names")
            
        elif self.model:
            # For QTextEdit, update the string list model
            self.model.setStringList(table_names)
            print(f"DEBUG SQLEditor: QTextEdit autocomplete updated with {len(table_names)} table names")
        
    def get_text(self) -> str:
        """Get the current text in the editor"""
        if QSCINTILLA_AVAILABLE:
            return self.editor.text()
        else:
            return self.editor.toPlainText()
            
    def set_text(self, text: str):
        """Set text in the editor"""
        if QSCINTILLA_AVAILABLE:
            self.editor.setText(text)
        else:
            self.editor.setPlainText(text)
    
    def get_selected_text(self) -> str:
        """Get the currently selected text in the editor"""
        if QSCINTILLA_AVAILABLE:
            return self.editor.selectedText()
        else:
            cursor = self.editor.textCursor()
            return cursor.selectedText()
    
    def has_selection(self) -> bool:
        """Check if there is any text selected in the editor"""
        if QSCINTILLA_AVAILABLE:
            return self.editor.hasSelectedText()
        else:
            cursor = self.editor.textCursor()
            return cursor.hasSelection()
    
    def apply_theme(self, theme_name: str):
        """Apply theme to the SQL editor"""
        self.current_theme = theme_name
        
        if QSCINTILLA_AVAILABLE and hasattr(self, 'lexer'):
            # Define theme colors for QsciScintilla
            theme_colors = self._get_theme_colors(theme_name)
            
            # Apply colors to the lexer
            from PyQt6.QtGui import QColor
            
            # Set paper (background) and default text color
            self.lexer.setPaper(QColor(theme_colors['background']))
            self.lexer.setColor(QColor(theme_colors['text']))
            
            # Set specific SQL syntax colors
            self.lexer.setColor(QColor(theme_colors['keyword']), self.lexer.Keyword)
            self.lexer.setColor(QColor(theme_colors['string']), self.lexer.SingleQuotedString)
            self.lexer.setColor(QColor(theme_colors['string']), self.lexer.DoubleQuotedString)
            self.lexer.setColor(QColor(theme_colors['comment']), self.lexer.Comment)
            self.lexer.setColor(QColor(theme_colors['comment']), self.lexer.CommentLine)
            self.lexer.setColor(QColor(theme_colors['number']), self.lexer.Number)
            self.lexer.setColor(QColor(theme_colors['operator']), self.lexer.Operator)
            
            # Set background for all styles
            for style in range(16):  # QsciLexerSQL has about 16 styles
                self.lexer.setPaper(QColor(theme_colors['background']), style)
            
            # Set editor background and selection colors
            self.editor.setCaretLineBackgroundColor(QColor(theme_colors['caret_line']))
            self.editor.setSelectionBackgroundColor(QColor(theme_colors['selection']))
            self.editor.setMarginLineNumbers(0, True)
            self.editor.setMarginWidth(0, 40)
            self.editor.setMarginsBackgroundColor(QColor(theme_colors['margin']))
            self.editor.setMarginsForegroundColor(QColor(theme_colors['margin_text']))
    
    def _get_theme_colors(self, theme_name: str) -> dict:
        """Get color scheme for the specified theme"""
        themes = {
            'light': {
                'background': '#ffffff',
                'text': '#333333',
                'keyword': '#0000ff',
                'string': '#008000',
                'comment': '#808080',
                'number': '#ff8000',
                'operator': '#800080',
                'caret_line': '#f0f0f0',
                'selection': '#e3f2fd',
                'margin': '#f8f8f8',
                'margin_text': '#666666'
            },
            'dark': {
                'background': '#2d2d2d',
                'text': '#ffffff',
                'keyword': '#569cd6',
                'string': '#ce9178',
                'comment': '#6a9955',
                'number': '#b5cea8',
                'operator': '#d4d4d4',
                'caret_line': '#404040',
                'selection': '#4a4a4a',
                'margin': '#2b2b2b',
                'margin_text': '#cccccc'
            },
            'blue': {
                'background': '#ffffff',
                'text': '#0d47a1',
                'keyword': '#1976d2',
                'string': '#388e3c',
                'comment': '#757575',
                'number': '#f57c00',
                'operator': '#7b1fa2',
                'caret_line': '#f3f9ff',
                'selection': '#bbdefb',
                'margin': '#e3f2fd',
                'margin_text': '#1565c0'
            },
            'green': {
                'background': '#ffffff',
                'text': '#1b5e20',
                'keyword': '#388e3c',
                'string': '#2e7d32',
                'comment': '#757575',
                'number': '#f57c00',
                'operator': '#7b1fa2',
                'caret_line': '#f1f8e9',
                'selection': '#c8e6c9',
                'margin': '#e8f5e8',
                'margin_text': '#2e7d32'
            }
        }
        return themes.get(theme_name, themes['light'])


class AutomationWidget(QWidget):
    """Widget for automation configuration and file loading"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent_gui = parent
        self.selected_files = []  # List of selected files/folders
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the automation widget UI"""
        layout = QVBoxLayout(self)
        
        # Title
        title_label = QLabel("Automation Configuration")
        title_label.setStyleSheet("font-size: 16px; font-weight: bold; margin-bottom: 10px;")
        layout.addWidget(title_label)
        
        # File Selection Group
        file_group = QGroupBox("File/Folder Selection")
        file_layout = QVBoxLayout(file_group)
        
        # Selection buttons
        button_layout = QHBoxLayout()
        
        self.add_csv_file_btn = QPushButton("Add CSV File")
        self.add_csv_file_btn.clicked.connect(self.add_csv_file)
        button_layout.addWidget(self.add_csv_file_btn)
        
        self.add_excel_file_btn = QPushButton("Add Excel File")
        self.add_excel_file_btn.clicked.connect(self.add_excel_file)
        button_layout.addWidget(self.add_excel_file_btn)
        
        self.add_csv_folder_btn = QPushButton("Add CSV Folder")
        self.add_csv_folder_btn.clicked.connect(self.add_csv_folder)
        button_layout.addWidget(self.add_csv_folder_btn)
        
        self.add_excel_folder_btn = QPushButton("Add Excel Folder")
        self.add_excel_folder_btn.clicked.connect(self.add_excel_folder)
        button_layout.addWidget(self.add_excel_folder_btn)
        
        # Second row of buttons for JSON and Parquet
        button_layout2 = QHBoxLayout()
        
        self.add_json_file_btn = QPushButton("Add JSON File")
        self.add_json_file_btn.clicked.connect(self.add_json_file)
        button_layout2.addWidget(self.add_json_file_btn)
        
        self.add_parquet_file_btn = QPushButton("Add Parquet File")
        self.add_parquet_file_btn.clicked.connect(self.add_parquet_file)
        button_layout2.addWidget(self.add_parquet_file_btn)
        
        self.add_jupyter_file_btn = QPushButton("Add Jupyter Notebook")
        self.add_jupyter_file_btn.clicked.connect(self.add_jupyter_file)
        button_layout2.addWidget(self.add_jupyter_file_btn)
        
        # Add spacer to align with first row
        button_layout2.addWidget(QLabel())  # Spacer
        
        file_layout.addLayout(button_layout)
        file_layout.addLayout(button_layout2)
        
        # Selected files list
        self.files_list = QTreeWidget()
        self.files_list.setHeaderLabels(["Type", "Path", "Table Name", "Config", "Status"])
        self.files_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.files_list.customContextMenuRequested.connect(self.show_file_context_menu)
        file_layout.addWidget(self.files_list)
        
        layout.addWidget(file_group)
        
        # Trigger Configuration Group
        trigger_group = QGroupBox("Trigger Configuration")
        trigger_layout = QVBoxLayout(trigger_group)
        
        # Buttons in horizontal layout
        buttons_layout = QHBoxLayout()
        
        # Manual trigger button
        self.load_all_btn = QPushButton("Load All Files Now")
        self.load_all_btn.clicked.connect(self.load_all_files)
        self.load_all_btn.setMaximumWidth(150)
        buttons_layout.addWidget(self.load_all_btn)
        
        # Save automation button
        self.save_automation_btn = QPushButton("Save Automation")
        self.save_automation_btn.clicked.connect(self.save_automation)
        self.save_automation_btn.setMaximumWidth(120)
        buttons_layout.addWidget(self.save_automation_btn)
        
        # Clear button
        self.clear_btn = QPushButton("Clear All")
        self.clear_btn.clicked.connect(self.clear_all_files)
        self.clear_btn.setMaximumWidth(80)
        buttons_layout.addWidget(self.clear_btn)
        
        trigger_layout.addLayout(buttons_layout)
        
        # Save/Load automation section
        save_load_layout = QVBoxLayout()
        
        # Load automation dropdown
        load_layout = QHBoxLayout()
        load_label = QLabel("Load Automation:")
        load_layout.addWidget(load_label)
        
        self.automation_dropdown = QComboBox()
        self.automation_dropdown.addItem("Select saved automation...")
        self.automation_dropdown.currentTextChanged.connect(self.load_selected_automation)
        load_layout.addWidget(self.automation_dropdown)
        
        # Initialize dropdown with available automations
        self.refresh_automation_dropdown()
        
        # Refresh button for dropdown
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.refresh_automation_dropdown)
        refresh_btn.setMaximumWidth(80)
        load_layout.addWidget(refresh_btn)
        
        save_load_layout.addLayout(load_layout)
        trigger_layout.addLayout(save_load_layout)
        
        # Status area
        self.status_text = QTextEdit()
        self.status_text.setMaximumHeight(100)
        self.status_text.setReadOnly(True)
        self.status_text.setPlaceholderText("Automation status will appear here...")
        trigger_layout.addWidget(self.status_text)
        
        layout.addWidget(trigger_group)
        
        # Stretch to fill remaining space
        layout.addStretch()
    
    def add_csv_file(self):
        """Add a CSV file to the automation list"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select CSV File", "", "CSV Files (*.csv)"
        )
        if file_path:
            self.add_file_to_list("CSV File", file_path)
    
    def add_excel_file(self):
        """Add an Excel file to the automation list"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Excel File", "", "Excel Files (*.xlsx *.xls)"
        )
        if file_path:
            self.add_file_to_list("Excel File", file_path)
    
    def add_csv_folder(self):
        """Add a CSV folder to the automation list"""
        folder_path = QFileDialog.getExistingDirectory(
            self, "Select CSV Folder"
        )
        if folder_path:
            self.add_file_to_list("CSV Folder", folder_path)
    
    def add_excel_folder(self):
        """Add an Excel folder to the automation list"""
        folder_path = QFileDialog.getExistingDirectory(
            self, "Select Excel Folder"
        )
        if folder_path:
            self.add_file_to_list("Excel Folder", folder_path)
    
    def add_json_file(self):
        """Add a JSON file to the automation list"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select JSON File", "", "JSON Files (*.json)"
        )
        if file_path:
            self.add_file_to_list("JSON File", file_path)
    
    def add_parquet_file(self):
        """Add a Parquet file to the automation list"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Parquet File", "", "Parquet Files (*.parquet *.pq)"
        )
        if file_path:
            self.add_file_to_list("Parquet File", file_path)
    
    def add_jupyter_file(self):
        """Add a Jupyter notebook file to the automation list"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Jupyter Notebook", "", "Jupyter Notebooks (*.ipynb)"
        )
        if file_path:
            self.add_file_to_list("Jupyter Notebook", file_path)
    
    def refresh_automation_dropdown(self):
        """Refresh the automation dropdown with available saved automations"""
        import os
        
        # Clear current items except the first placeholder
        self.automation_dropdown.clear()
        self.automation_dropdown.addItem("Select saved automation...")
        
        # Get automations folder
        automations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "automations")
        
        if os.path.exists(automations_dir):
            # Add all .json files in the automations folder
            for filename in os.listdir(automations_dir):
                if filename.endswith('.json'):
                    self.automation_dropdown.addItem(filename[:-5])  # Remove .json extension
    
    def load_selected_automation(self, automation_name):
        """Load the selected automation from dropdown"""
        if automation_name == "Select saved automation..." or not automation_name:
            return
        
        import os
        import json
        
        # Construct file path
        automations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "automations")
        file_path = os.path.join(automations_dir, f"{automation_name}.json")
        
        if not os.path.exists(file_path):
            self.log_status(f"Error: Automation file not found: {automation_name}.json")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                automation_data = json.load(f)
            
            # Validate JSON structure
            if "files" not in automation_data:
                raise ValueError("Invalid automation file format: missing 'files' key")
            
            # Clear current list
            self.files_list.clear()
            self.selected_files.clear()
            
            # Load files from JSON
            loaded_count = 0
            skipped_count = 0
            
            for file_data in automation_data["files"]:
                file_path_to_check = file_data.get("path", "")
                
                # Check if file/folder exists
                if not os.path.exists(file_path_to_check):
                    self.log_status(f"Warning: File/folder not found, skipping: {file_path_to_check}")
                    skipped_count += 1
                    continue
                
                # Create tree item
                item = QTreeWidgetItem([
                    file_data.get("type", "Unknown"),
                    file_path_to_check,
                    file_data.get("table_name", ""),
                    self._format_config_display(file_data.get("config", {})),
                    "Ready"
                ])
                
                # Store configuration data
                item.setData(0, Qt.ItemDataRole.UserRole, file_data.get("config", {}))
                
                self.files_list.addTopLevelItem(item)
                
                # Add to selected_files list
                file_info = {
                    'type': file_data.get("type", "Unknown"),
                    'path': file_path_to_check,
                    'table_name': file_data.get("table_name", ""),
                    'config': file_data.get("config", {}),
                    'item': item
                }
                self.selected_files.append(file_info)
                loaded_count += 1
            
            # Resize columns
            for i in range(5):
                self.files_list.resizeColumnToContents(i)
            
            # Reset dropdown to placeholder
            self.automation_dropdown.setCurrentIndex(0)
            
            # Log results
            if loaded_count > 0:
                self.log_status(f"Loaded {loaded_count} files from automation: {automation_name}")
            if skipped_count > 0:
                self.log_status(f"Skipped {skipped_count} missing files")
                
        except Exception as e:
            error_msg = f"Error loading automation {automation_name}: {str(e)}"
            self.log_status(error_msg)
            QMessageBox.critical(self, "Load Error", error_msg)
    
    def add_file_to_list(self, file_type, file_path):
        """Add a file or folder to the automation list"""
        # Generate table name from file/folder path
        path_obj = Path(file_path)
        if file_type.endswith("Folder"):
            table_name = f"{path_obj.name}_data"
        else:
            table_name = path_obj.stem
        
        # Set default configuration based on file type
        config_text = "Default"
        config_data = {}
        
        if "CSV" in file_type:
            config_data = {'delimiter': ','}
            config_text = "Delimiter: ,"
        elif "Excel" in file_type:
            config_data = {'sheet_name': None}
            config_text = "Sheet: Auto"
        elif "JSON" in file_type:
            config_data = {}
            config_text = "Default"
        elif "Parquet" in file_type:
            config_data = {}
            config_text = "Default"
        elif "Jupyter" in file_type:
            config_data = {}
            config_text = "Execute notebook"
        
        # Create tree item
        item = QTreeWidgetItem(self.files_list)
        item.setText(0, file_type)
        item.setText(1, file_path)
        item.setText(2, table_name)
        item.setText(3, config_text)
        item.setText(4, "Ready")
        
        # Store file info
        file_info = {
            'type': file_type,
            'path': file_path,
            'table_name': table_name,
            'config': config_data,
            'item': item
        }
        self.selected_files.append(file_info)
        
        # Resize columns to fit content
        self.files_list.resizeColumnToContents(0)
        self.files_list.resizeColumnToContents(1)
        self.files_list.resizeColumnToContents(2)
        self.files_list.resizeColumnToContents(3)
        
        self.log_status(f"Added {file_type}: {file_path}")
    
    def show_file_context_menu(self, position):
        """Show context menu for file list"""
        item = self.files_list.itemAt(position)
        if item:
            menu = QMenu(self)
            
            remove_action = menu.addAction("Remove")
            edit_table_name_action = menu.addAction("Edit Table Name")
            
            # Add configuration options based on file type
            file_type = item.text(0)
            config_action = None
            
            if "CSV" in file_type:
                config_action = menu.addAction("Configure CSV Delimiter")
            elif "Excel" in file_type:
                config_action = menu.addAction("Configure Excel Sheet")
            
            action = menu.exec(self.files_list.mapToGlobal(position))
            
            if action == remove_action:
                self.remove_file_item(item)
            elif action == edit_table_name_action:
                self.edit_table_name(item)
            elif action == config_action:
                self.configure_file_options(item)
    
    def remove_file_item(self, item):
        """Remove a file item from the list"""
        # Find and remove from selected_files list
        for i, file_info in enumerate(self.selected_files):
            if file_info['item'] == item:
                self.selected_files.pop(i)
                break
        
        # Remove from tree widget
        index = self.files_list.indexOfTopLevelItem(item)
        if index >= 0:
            self.files_list.takeTopLevelItem(index)
        
        self.log_status(f"Removed: {item.text(1)}")
    
    def edit_table_name(self, item):
        """Edit the table name for a file item"""
        current_name = item.text(2)
        new_name, ok = QInputDialog.getText(
            self, "Edit Table Name", "Table Name:", text=current_name
        )
        
        if ok and new_name.strip():
            # Update item
            item.setText(2, new_name.strip())
            
            # Update selected_files list
            for file_info in self.selected_files:
                if file_info['item'] == item:
                    file_info['table_name'] = new_name.strip()
                    break
            
            self.log_status(f"Updated table name to: {new_name.strip()}")
    
    def configure_file_options(self, item):
        """Configure file-specific options like CSV delimiter or Excel sheet"""
        file_type = item.text(0)
        
        # Find the file info
        file_info = None
        for info in self.selected_files:
            if info['item'] == item:
                file_info = info
                break
        
        if not file_info:
            return
        
        if "CSV" in file_type:
            # Configure CSV options with a comprehensive dialog
            dialog = CSVConfigDialog(self, file_info['config'])
            if dialog.exec() == QDialog.DialogCode.Accepted:
                # Update configuration with all CSV settings
                config = dialog.get_config()
                file_info['config'].update(config)
                
                # Update display text with key settings
                config_text = f"Delimiter: {repr(config.get('delimiter', ','))}"
                if config.get('quote_char'):
                    config_text += f", Quote: {repr(config['quote_char'])}"
                if not config.get('has_header', True):
                    config_text += ", No Header"
                if config.get('encoding', 'utf8') != 'utf8':
                    config_text += f", Encoding: {config['encoding']}"
                    
                item.setText(3, config_text)
                self.log_status(f"Updated CSV configuration: {config_text}")
        
        elif "Excel" in file_type:
            # Configure Excel sheet name
            current_sheet = file_info['config'].get('sheet_name', None)
            sheet_text = current_sheet if current_sheet else ""
            
            sheet_name, ok = QInputDialog.getText(
                self, "Configure Excel Sheet", 
                "Enter sheet name (leave empty for auto-detection):", 
                text=sheet_text
            )
            
            if ok:
                if sheet_name.strip():
                    file_info['config']['sheet_name'] = sheet_name.strip()
                    item.setText(3, f"Sheet: {sheet_name.strip()}")
                    self.log_status(f"Updated Excel sheet to: {sheet_name.strip()}")
                else:
                    file_info['config']['sheet_name'] = None
                    item.setText(3, "Sheet: Auto")
                    self.log_status("Updated Excel sheet to auto-detection")
    
    def load_all_files(self):
        """Load all files in the automation list"""
        if not self.selected_files:
            self.log_status("No files selected for loading.")
            return
        
        self.log_status(f"Starting to load {len(self.selected_files)} items...")
        
        for file_info in self.selected_files:
            try:
                file_info['item'].setText(3, "Loading...")
                self.load_single_file(file_info)
                file_info['item'].setText(3, "Loaded")
            except Exception as e:
                file_info['item'].setText(3, "Error")
                self.log_status(f"Error loading {file_info['path']}: {str(e)}")
        
        # Refresh database tree
        if self.parent_gui:
            self.parent_gui.refresh_database_tree()
        
        self.log_status("Finished loading all files.")
    
    def load_single_file(self, file_info):
        """Load a single file or folder"""
        file_type = file_info['type']
        file_path = file_info['path']
        table_name = file_info['table_name']
        config = file_info.get('config', {})
        
        if file_type == "CSV File":
            delimiter = config.get('delimiter', ',')
            self.parent_gui.load_csv_file_with_delimiter(file_path, table_name, delimiter)
        elif file_type == "Excel File":
            sheet_name = config.get('sheet_name', None)
            self.parent_gui.load_excel_file_with_sheet(file_path, table_name, sheet_name)
        elif file_type == "CSV Folder":
            delimiter = config.get('delimiter', ',')
            quote_char = config.get('quote_char', None)
            has_header = config.get('has_header', True)
            encoding = config.get('encoding', 'utf8')
            self.parent_gui.load_csv_folder_with_delimiter(file_path, table_name, delimiter, quote_char, has_header, encoding)
        elif file_type == "Excel Folder":
            sheet_name = config.get('sheet_name', None)
            self.parent_gui.load_excel_folder(file_path, table_name, sheet_name)
        elif file_type == "JSON File":
            self.parent_gui.load_json_file(file_path, table_name)
        elif file_type == "Parquet File":
            self.parent_gui.load_parquet_file(file_path, table_name)
        elif file_type == "Jupyter Notebook":
            self.execute_jupyter_notebook(file_path)
        
        if file_type != "Jupyter Notebook":
            self.log_status(f"Loaded {file_type}: {file_path} as table '{table_name}'")
        else:
            self.log_status(f"Executed {file_type}: {file_path}")
    
    def execute_jupyter_notebook(self, notebook_path):
        """Execute a Jupyter notebook file"""
        import subprocess
        import os
        
        try:
            # Check if jupyter is available
            result = subprocess.run(['jupyter', '--version'], capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception("Jupyter is not installed or not available in PATH")
            
            # Execute the notebook
            self.log_status(f"Executing Jupyter notebook: {notebook_path}")
            
            # Use nbconvert to execute the notebook
            cmd = [
                'jupyter', 'nbconvert', 
                '--to', 'notebook',
                '--execute',
                '--inplace',
                notebook_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(notebook_path))
            
            if result.returncode == 0:
                self.log_status(f"Successfully executed notebook: {os.path.basename(notebook_path)}")
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                self.log_status(f"Error executing notebook {os.path.basename(notebook_path)}: {error_msg}")
                raise Exception(f"Notebook execution failed: {error_msg}")
                
        except FileNotFoundError:
            error_msg = "Jupyter not found. Please install Jupyter: pip install jupyter"
            self.log_status(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error executing notebook: {str(e)}"
            self.log_status(error_msg)
            raise Exception(error_msg)
    
    def clear_all_files(self):
        """Clear all files from the automation list"""
        if self.files_list.topLevelItemCount() == 0:
            self.log_status("No files to clear.")
            return
        
        # Ask for confirmation
        reply = QMessageBox.question(
            self, 
            "Clear All Files", 
            f"Are you sure you want to clear all {self.files_list.topLevelItemCount()} files from the automation list?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Clear the tree widget
            self.files_list.clear()
            
            # Clear the selected files list
            self.selected_files.clear()
            
            self.log_status("All files cleared from automation list.")
    
    def log_status(self, message):
        """Log a status message"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.status_text.append(f"[{timestamp}] {message}")
    
    def save_automation(self):
        """Save current automation configuration to JSON file"""
        if self.files_list.topLevelItemCount() == 0:
            QMessageBox.information(self, "No Files", "No files to save. Please add files to the automation list first.")
            return
        
        # Get filename from user
        filename, ok = QInputDialog.getText(
            self, "Save Automation", "Enter automation name (without .json extension):"
        )
        
        if not ok or not filename.strip():
            return
        
        # Ensure .json extension
        if not filename.endswith('.json'):
            filename += '.json'
        
        # Create full path in automations folder
        import os
        automations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "automations")
        file_path = os.path.join(automations_dir, filename)
        
        try:
            automation_data = {
                "version": "1.0",
                "files": []
            }
            
            # Serialize all files in the list
            for i in range(self.files_list.topLevelItemCount()):
                item = self.files_list.topLevelItem(i)
                file_data = {
                    "type": item.text(0),
                    "path": item.text(1),
                    "table_name": item.text(2),
                    "config": item.data(0, Qt.ItemDataRole.UserRole) or {}
                }
                automation_data["files"].append(file_data)
            
            # Write to JSON file
            import json
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(automation_data, f, indent=2, ensure_ascii=False)
            
            self.log_status(f"Automation configuration saved to {file_path}")
            QMessageBox.information(self, "Success", f"Automation configuration saved successfully to:\n{file_path}")
            
        except Exception as e:
            error_msg = f"Error saving automation configuration: {str(e)}"
            self.log_status(error_msg)
            QMessageBox.critical(self, "Save Error", error_msg)
    
    def load_automation(self):
        """Load automation configuration from JSON file"""
        import os
        automations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "automations")
        
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Load Automation Configuration", automations_dir, "JSON Files (*.json)"
        )
        
        if not file_path:
            return
        
        try:
            import json
            import os
            
            with open(file_path, 'r', encoding='utf-8') as f:
                automation_data = json.load(f)
            
            # Validate JSON structure
            if "files" not in automation_data:
                raise ValueError("Invalid automation file format: missing 'files' key")
            
            # Clear current list
            self.files_list.clear()
            
            # Load files from JSON
            loaded_count = 0
            skipped_count = 0
            
            for file_data in automation_data["files"]:
                file_path_to_check = file_data.get("path", "")
                
                # Check if file/folder exists
                if not os.path.exists(file_path_to_check):
                    self.log_status(f"Warning: File/folder not found, skipping: {file_path_to_check}")
                    skipped_count += 1
                    continue
                
                # Create tree item
                item = QTreeWidgetItem([
                    file_data.get("type", "Unknown"),
                    file_path_to_check,
                    file_data.get("table_name", ""),
                    self._format_config_display(file_data.get("config", {})),
                    "Ready"
                ])
                
                # Store configuration data
                item.setData(0, Qt.ItemDataRole.UserRole, file_data.get("config", {}))
                
                self.files_list.addTopLevelItem(item)
                loaded_count += 1
            
            # Resize columns
            for i in range(self.files_list.columnCount()):
                self.files_list.resizeColumnToContents(i)
            
            success_msg = f"Loaded {loaded_count} files from automation configuration"
            if skipped_count > 0:
                success_msg += f" ({skipped_count} files skipped - not found)"
            
            self.log_status(success_msg)
            QMessageBox.information(self, "Success", success_msg)
            
        except Exception as e:
            error_msg = f"Error loading automation configuration: {str(e)}"
            self.log_status(error_msg)
            QMessageBox.critical(self, "Load Error", error_msg)
    
    def _format_config_display(self, config):
        """Format configuration data for display in the Config column"""
        if not config:
            return "Default"
        
        display_parts = []
        if "delimiter" in config:
            delimiter = config["delimiter"]
            if delimiter == "\t":
                display_parts.append("Tab")
            elif delimiter == " ":
                display_parts.append("Space")
            else:
                display_parts.append(f"'{delimiter}'")
        
        if "sheet_name" in config:
            sheet = config["sheet_name"]
            if sheet:
                display_parts.append(f"Sheet: {sheet}")
            else:
                display_parts.append("Auto-detect")
        
        return ", ".join(display_parts) if display_parts else "Default"


class ResultsTableWidget(QWidget):
    """Custom widget for displaying paginated query results"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_page = 0
        self.total_count = 0
        self.page_size = 1000
        self.current_query = ""
        self.current_columns = []
        self.parent_gui = parent
        self.setup_ui()
        
    def setup_ui(self):
        """Setup the results widget with table and pagination controls"""
        layout = QVBoxLayout(self)
        
        # Results table with performance optimizations
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        
        # Performance optimizations for smooth resizing
        self.table.setUpdatesEnabled(True)  # Keep updates enabled by default
        self.table.setSortingEnabled(False)  # Disable sorting for better performance
        self.table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        
        # Enable context menu
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        
        layout.addWidget(self.table)
        
        # Pagination controls
        pagination_layout = QHBoxLayout()
        
        self.page_info_label = QLabel("No results")
        pagination_layout.addWidget(self.page_info_label)
        
        pagination_layout.addStretch()
        
        # Page size selector
        pagination_layout.addWidget(QLabel("Page size:"))
        self.page_size_combo = QComboBox()
        self.page_size_combo.addItems(["100", "500", "1000", "2000", "5000"])
        self.page_size_combo.setCurrentText("1000")
        self.page_size_combo.currentTextChanged.connect(self.on_page_size_changed)
        pagination_layout.addWidget(self.page_size_combo)
        
        # Navigation buttons
        self.first_btn = QPushButton("First")
        self.first_btn.clicked.connect(self.go_to_first_page)
        pagination_layout.addWidget(self.first_btn)
        
        self.prev_btn = QPushButton("Previous")
        self.prev_btn.clicked.connect(self.go_to_previous_page)
        pagination_layout.addWidget(self.prev_btn)
        
        self.next_btn = QPushButton("Next")
        self.next_btn.clicked.connect(self.go_to_next_page)
        pagination_layout.addWidget(self.next_btn)
        
        self.last_btn = QPushButton("Last")
        self.last_btn.clicked.connect(self.go_to_last_page)
        pagination_layout.addWidget(self.last_btn)
        
        layout.addLayout(pagination_layout)
        
        # Initially disable pagination controls
        self.update_pagination_controls()
        
    def display_results(self, data: List[tuple], columns: List[str], total_count: int = -1, current_page: int = 0, query: str = ""):
        """Display query results in the table with pagination info"""
        self.current_columns = columns
        self.total_count = total_count
        self.current_page = current_page
        self.current_query = query
        
        # Temporarily disable updates for better performance
        self.table.setUpdatesEnabled(False)
        
        try:
            # Update table
            self.table.setRowCount(len(data))
            self.table.setColumnCount(len(columns))
            self.table.setHorizontalHeaderLabels(columns)
            
            # Batch insert items for better performance
            for row_idx, row_data in enumerate(data):
                for col_idx, cell_data in enumerate(row_data):
                    item = QTableWidgetItem(str(cell_data) if cell_data is not None else "")
                    self.table.setItem(row_idx, col_idx, item)
            
            # Resize columns to contents only if dataset is small
            if len(data) <= 100:
                self.table.resizeColumnsToContents()
            else:
                # For large datasets, use uniform column width for better performance
                header = self.table.horizontalHeader()
                for i in range(len(columns)):
                    header.resizeSection(i, 120)  # Set uniform width
                    
        finally:
            # Re-enable updates
            self.table.setUpdatesEnabled(True)
        
        # Update pagination info
        self.update_pagination_info()
        self.update_pagination_controls()
        
    def update_pagination_info(self):
        """Update the pagination information label"""
        if self.total_count == -1:
            # Non-paginated results
            row_count = self.table.rowCount()
            self.page_info_label.setText(f"Showing {row_count} rows")
        elif self.total_count == 0:
            self.page_info_label.setText("No results")
        else:
            # Paginated results
            start_row = self.current_page * self.page_size + 1
            end_row = min((self.current_page + 1) * self.page_size, self.total_count)
            total_pages = (self.total_count + self.page_size - 1) // self.page_size
            self.page_info_label.setText(f"Showing {start_row}-{end_row} of {self.total_count} rows (Page {self.current_page + 1} of {total_pages})")
            
    def update_pagination_controls(self):
        """Enable/disable pagination controls based on current state"""
        has_pagination = self.total_count > self.page_size and self.total_count != -1
        
        self.first_btn.setEnabled(has_pagination and self.current_page > 0)
        self.prev_btn.setEnabled(has_pagination and self.current_page > 0)
        self.next_btn.setEnabled(has_pagination and (self.current_page + 1) * self.page_size < self.total_count)
        self.last_btn.setEnabled(has_pagination and (self.current_page + 1) * self.page_size < self.total_count)
        
    def on_page_size_changed(self, new_size_text):
        """Handle page size change"""
        self.page_size = int(new_size_text)
        if self.current_query and self.total_count > 0:
            self.current_page = 0  # Reset to first page
            self.load_page(0)
            
    def go_to_first_page(self):
        """Go to the first page"""
        self.load_page(0)
        
    def go_to_previous_page(self):
        """Go to the previous page"""
        if self.current_page > 0:
            self.load_page(self.current_page - 1)
            
    def go_to_next_page(self):
        """Go to the next page"""
        max_page = (self.total_count + self.page_size - 1) // self.page_size - 1
        if self.current_page < max_page:
            self.load_page(self.current_page + 1)
            
    def go_to_last_page(self):
        """Go to the last page"""
        max_page = (self.total_count + self.page_size - 1) // self.page_size - 1
        self.load_page(max_page)
        
    def load_page(self, page_number):
        """Load a specific page of results"""
        if self.parent_gui and self.current_query:
            self.parent_gui.execute_paginated_query(self.current_query, page_number, self.page_size)
        

    def clear_results(self):
        """Clear the results table"""
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.current_page = 0
        self.total_count = 0
        self.current_query = ""
        self.current_columns = []
        self.update_pagination_info()
        self.update_pagination_controls()
        
    def show_context_menu(self, position):
        """Show context menu for copying data"""
        if self.table.rowCount() == 0 or self.table.columnCount() == 0:
            return
            
        menu = QMenu(self)
        
        # Get current selection
        current_item = self.table.itemAt(position)
        if current_item:
            # Copy single value
            copy_value_action = QAction("Copy Value", self)
            copy_value_action.triggered.connect(lambda: self.copy_single_value(current_item))
            menu.addAction(copy_value_action)
            
            # Copy row with headers
            copy_row_action = QAction("Copy Row with Headers", self)
            copy_row_action.triggered.connect(lambda: self.copy_row_with_headers(current_item.row()))
            menu.addAction(copy_row_action)
            
            # Copy column with headers
            copy_column_action = QAction("Copy Column with Headers", self)
            copy_column_action.triggered.connect(lambda: self.copy_column_with_headers(current_item.column()))
            menu.addAction(copy_column_action)
            
            menu.addSeparator()
            
            # Copy entire table
            copy_table_action = QAction("Copy Table", self)
            copy_table_action.triggered.connect(self.copy_entire_table)
            menu.addAction(copy_table_action)
            
        menu.exec(self.table.mapToGlobal(position))
        
    def copy_single_value(self, item):
        """Copy the value of a single cell to clipboard"""
        if item:
            clipboard = QApplication.clipboard()
            clipboard.setText(item.text())
            
    def copy_row_with_headers(self, row_index):
        """Copy a row with column headers to clipboard"""
        if row_index < 0 or row_index >= self.table.rowCount():
            return
            
        clipboard = QApplication.clipboard()
        
        # Get headers
        headers = []
        for col in range(self.table.columnCount()):
            header_item = self.table.horizontalHeaderItem(col)
            headers.append(header_item.text() if header_item else f"Column_{col}")
            
        # Get row data
        row_data = []
        for col in range(self.table.columnCount()):
            item = self.table.item(row_index, col)
            row_data.append(item.text() if item else "")
            
        # Format as tab-separated values
        header_line = "\t".join(headers)
        data_line = "\t".join(row_data)
        result = f"{header_line}\n{data_line}"
        
        clipboard.setText(result)
        
    def copy_column_with_headers(self, column_index):
        """Copy a column with header to clipboard"""
        if column_index < 0 or column_index >= self.table.columnCount():
            return
            
        clipboard = QApplication.clipboard()
        
        # Get header
        header_item = self.table.horizontalHeaderItem(column_index)
        header = header_item.text() if header_item else f"Column_{column_index}"
        
        # Get column data
        column_data = [header]
        for row in range(self.table.rowCount()):
            item = self.table.item(row, column_index)
            column_data.append(item.text() if item else "")
            
        # Join with newlines
        result = "\n".join(column_data)
        
        clipboard.setText(result)
        
    def copy_entire_table(self):
        """Copy the entire table with headers to clipboard"""
        if self.table.rowCount() == 0 or self.table.columnCount() == 0:
            return
            
        clipboard = QApplication.clipboard()
        
        # Get headers
        headers = []
        for col in range(self.table.columnCount()):
            header_item = self.table.horizontalHeaderItem(col)
            headers.append(header_item.text() if header_item else f"Column_{col}")
            
        # Start with headers
        table_data = ["\t".join(headers)]
        
        # Get all row data
        for row in range(self.table.rowCount()):
            row_data = []
            for col in range(self.table.columnCount()):
                item = self.table.item(row, col)
                row_data.append(item.text() if item else "")
            table_data.append("\t".join(row_data))
            
        # Join all rows with newlines
        result = "\n".join(table_data)
        
        clipboard.setText(result)


class DuckDBGUI(QMainWindow):
    """Main application window"""
    
    def __init__(self):
        super().__init__()
        self.connection = None
        self.query_worker = None
        self.current_database = 'local'  # Track current database context
        self.current_connection = 'local'  # Track current connection context
        self.current_table_names = []  # Store current table names for autocomplete
        
        # Initialize theme manager
        self.theme_manager = ThemeManager()
        
        self.setup_database()
        
        # Initialize database connection manager
        self.connection_manager = DatabaseConnectionManager(self.connection)
        
        self.setup_ui()
        self.setup_menu_bar()
        self.setup_toolbar()
        self.setup_status_bar()
        
        # Apply initial theme
        self.apply_theme(self.theme_manager.get_current_theme())
        
        # Initial refresh to populate autocomplete
        self.refresh_database_tree()
        
        # Initialize Flask web server for PivotJS visualization
        self.setup_web_server()
        
    def setup_database(self):
        """Initialize DuckDB connection"""
        try:
            # Create a temporary database file in the application directory
            import tempfile
            import os
            
            # Create temp directory if it doesn't exist
            app_dir = os.path.dirname(os.path.abspath(__file__))
            temp_dir = os.path.join(app_dir, 'temp')
            os.makedirs(temp_dir, exist_ok=True)
            
            # Create temporary database file
            db_file = os.path.join(temp_dir, 'duckdb_gui_temp.duckdb')
            
            self.connection = duckdb.connect(db_file)
            # Create a named database called 'local' for easier referencing
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS local")
            self.connection.execute("USE local")
            print(f"DuckDB connection established with temporary database: {db_file}")
        except Exception as e:
            QMessageBox.critical(self, "Database Error", f"Failed to connect to DuckDB: {e}")
            
    def setup_ui(self):
        """Setup the main user interface"""
        self.setWindowTitle("DuckDB SQL GUI")
        self.setGeometry(100, 100, 1400, 800)
        
        # Initialize query results mapping
        self.query_results_tables = {}  # Maps query tab index to results table
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QHBoxLayout(central_widget)
        
        # Create main splitter with optimized settings
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.setOpaqueResize(True)  # Enable smooth resizing
        main_splitter.setChildrenCollapsible(False)  # Prevent panels from collapsing completely
        main_layout.addWidget(main_splitter)
        
        # Left panel - Database tree
        self.db_tree = DatabaseTreeWidget(self)
        main_splitter.addWidget(self.db_tree)
        
        # Right panel - SQL editor and results
        right_splitter = QSplitter(Qt.Orientation.Vertical)
        right_splitter.setOpaqueResize(True)  # Enable smooth resizing
        right_splitter.setChildrenCollapsible(False)  # Prevent panels from collapsing completely
        main_splitter.addWidget(right_splitter)
        
        # SQL Editor Tabs
        self.query_tabs = QTabWidget()
        self.query_tabs.setTabsClosable(True)
        self.query_tabs.tabCloseRequested.connect(self.close_query_tab)
        self.query_tabs.currentChanged.connect(self.on_query_tab_changed)
        
        right_splitter.addWidget(self.query_tabs)
        
        # Results area with single result display and tabs for messages/automation
        self.results_tabs = QTabWidget()
        
        # Create single results table widget
        self.single_results_table = ResultsTableWidget()
        self.single_results_table.parent_gui = self
        self.results_tabs.addTab(self.single_results_table, "Results")
        
        # Create first query tab (after results_tabs is initialized)
        self.add_new_query_tab("Query 1")
        
        # Messages/Log area
        self.messages_text = QTextEdit()
        self.messages_text.setMaximumHeight(150)
        self.messages_text.setReadOnly(True)
        self.results_tabs.addTab(self.messages_text, "Messages")
        
        # Automation tab
        self.automation_widget = AutomationWidget()
        self.automation_widget.parent_gui = self
        self.results_tabs.addTab(self.automation_widget, "Automation")
        
        right_splitter.addWidget(self.results_tabs)
        
        # Set splitter proportions and minimum sizes
        main_splitter.setSizes([300, 1100])
        main_splitter.setStretchFactor(0, 0)  # Database tree doesn't stretch
        main_splitter.setStretchFactor(1, 1)  # Right panel stretches
        
        right_splitter.setSizes([400, 400])
        right_splitter.setStretchFactor(0, 1)  # SQL editor stretches
        right_splitter.setStretchFactor(1, 0)  # Results area has fixed behavior
        
        # Set minimum sizes to prevent panels from becoming too small
        self.db_tree.setMinimumWidth(200)
        self.query_tabs.setMinimumHeight(30)  # Allow collapsing to one line
        self.results_tabs.setMinimumHeight(100)
        
        # Add some sample SQL to first tab
        sample_sql = """-- Welcome to DuckDB SQL GUI
-- Load files using the File menu or toolbar
-- Example queries:

SELECT 'Hello, DuckDB!' as message;

-- Show all tables
SHOW TABLES;

-- Describe a table structure
-- DESCRIBE table_name;
"""
        current_editor = self.get_current_editor()
        if current_editor:
            current_editor.set_text(sample_sql)
    
    def add_new_query_tab(self, tab_name="New Query"):
        """Add a new query tab with SQL editor"""
        editor = SQLEditor(self)
        
        # Apply current theme if available
        if hasattr(self, 'current_theme'):
            editor.apply_theme(self.current_theme)
        elif hasattr(self, 'theme_manager'):
            editor.apply_theme(self.theme_manager.current_theme)
        
        # Update table names for autocomplete using stored table names
        if hasattr(self, 'current_table_names'):
            editor.update_table_names(self.current_table_names)
        else:
            # If no table names are stored yet, initialize with empty list
            editor.update_table_names([])
        
        tab_index = self.query_tabs.addTab(editor, tab_name)
        
        # Initialize empty results data for this query tab
        self.query_results_tables[tab_index] = {
            'data': [],
            'columns': [],
            'total_count': 0,
            'current_page': 0,
            'query': ''
        }
        
        self.query_tabs.setCurrentIndex(tab_index)
        
        # Enable context menu for tab bar (only set once)
        if not hasattr(self, '_context_menu_enabled'):
            self.query_tabs.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self.query_tabs.customContextMenuRequested.connect(self.show_tab_context_menu)
            self._context_menu_enabled = True
        
        # Add sample SQL for new tabs (except the first one which gets it in setup_ui)
        if self.query_tabs.count() > 1:
            sample_sql = """-- New Query Tab
-- Write your SQL queries here

-- Example:
-- SELECT * FROM your_table LIMIT 10;
"""
            editor.set_text(sample_sql)
        
        return editor
    
    def on_query_tab_changed(self, index):
        """Handle query tab change to show corresponding results"""
        if index >= 0 and index in self.query_results_tables:
            # Get stored results data for this query tab
            results_data = self.query_results_tables[index]
            
            # Display the results in the single results table
            if results_data['data'] or results_data['columns']:
                self.single_results_table.display_results(
                    results_data['data'],
                    results_data['columns'],
                    results_data['total_count'],
                    results_data['current_page'],
                    results_data['query']
                )
            else:
                # Clear results if no data for this tab
                self.single_results_table.clear_results()
            
            # Switch to Results tab to show the data
            self.results_tabs.setCurrentIndex(0)
    
    def close_query_tab(self, index):
        """Close a query tab"""
        if self.query_tabs.count() > 1:  # Keep at least one tab
            # Remove the corresponding results data
            if index in self.query_results_tables:
                # Remove from mapping
                del self.query_results_tables[index]
                
                # Update indices in the mapping for tabs after the closed one
                updated_mapping = {}
                for tab_index, results_data in self.query_results_tables.items():
                    if tab_index > index:
                        updated_mapping[tab_index - 1] = results_data
                    else:
                        updated_mapping[tab_index] = results_data
                self.query_results_tables = updated_mapping
            
            self.query_tabs.removeTab(index)
            
            # Update the results display for the new current tab
            current_index = self.query_tabs.currentIndex()
            self.on_query_tab_changed(current_index)
        else:
            # Don't allow closing the last tab - show a message instead
            QMessageBox.information(self, "Cannot Close Tab", 
                                  "Cannot close the last query tab. At least one tab must remain open.")
            return
    
    def get_current_editor(self):
        """Get the currently active SQL editor"""
        current_widget = self.query_tabs.currentWidget()
        if isinstance(current_widget, SQLEditor):
            return current_widget
        return None
    
    def get_current_tab_name(self):
        """Get the name of the current tab"""
        current_index = self.query_tabs.currentIndex()
        return self.query_tabs.tabText(current_index)
    
    def set_current_tab_name(self, name):
        """Set the name of the current tab"""
        current_index = self.query_tabs.currentIndex()
        self.query_tabs.setTabText(current_index, name)
    
    def update_all_editors_table_names(self, table_names):
        """Update table names for autocomplete in all editor tabs"""
        self.log_message(f"DEBUG: Updating autocomplete for {self.query_tabs.count()} tabs with {len(table_names)} table names: {table_names[:10]}...")  # Show first 10 table names
        for i in range(self.query_tabs.count()):
            editor = self.query_tabs.widget(i)
            if isinstance(editor, SQLEditor):
                self.log_message(f"DEBUG: Updating tab {i} autocomplete")
                editor.update_table_names(table_names)
    
    def apply_theme_to_all_editors(self, theme_name):
        """Apply theme to all editor tabs"""
        for i in range(self.query_tabs.count()):
            editor = self.query_tabs.widget(i)
            if isinstance(editor, SQLEditor):
                editor.apply_theme(theme_name)
    
    def new_query(self):
        """Create a new query tab"""
        query_count = self.query_tabs.count() + 1
        tab_name = f"Query {query_count}"
        self.add_new_query_tab(tab_name)
        self.log_message(f"Created new query tab: {tab_name}")
    
    def open_query(self):
        """Open a saved query from JSON file"""
        import os
        
        # Get queries directory
        queries_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries')
        
        if not os.path.exists(queries_dir):
            QMessageBox.information(self, "No Queries", "No saved queries found.")
            return
        
        # Get list of saved queries
        query_files = [f for f in os.listdir(queries_dir) if f.endswith('.json')]
        
        if not query_files:
            QMessageBox.information(self, "No Queries", "No saved queries found.")
            return
        
        # Show list of queries to choose from
        query_names = [os.path.splitext(f)[0] for f in query_files]
        query_name, ok = QInputDialog.getItem(
            self,
            "Open Query",
            "Select a query to open:",
            query_names,
            0,
            False
        )
        
        if ok and query_name:
            try:
                import json
                file_path = os.path.join(queries_dir, f"{query_name}.json")
                
                with open(file_path, 'r', encoding='utf-8') as file:
                    query_data = json.load(file)
                
                # Create new tab with query name
                editor = self.add_new_query_tab(query_name)
                editor.set_text(query_data['content'])
                
                # Store the query name for saving
                editor.query_name = query_name
                
                self.log_message(f"Opened query: {query_name}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to open query: {str(e)}")
    
    def save_query_as(self):
        """Save the current query with a new name"""
        current_editor = self.get_current_editor()
        if not current_editor:
            return
        
        query_name, ok = QInputDialog.getText(
            self,
            "Save Query As",
            "Enter query name:"
        )
        
        if ok and query_name.strip():
            query_name = query_name.strip()
            if self._save_query_to_json(query_name, current_editor.get_text()):
                current_editor.query_name = query_name
                # Update tab title to show query name
                self.set_current_tab_name(query_name)
    
    def _save_to_file(self, file_path, content):
        """Helper method to save content to file"""
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(content)
            
            import os
            filename = os.path.basename(file_path)
            self.log_message(f"Saved query to: {filename}")
            return True
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save file: {str(e)}")
            return False
    
    def _save_query_to_json(self, query_name, content):
        """Helper method to save query as JSON file"""
        try:
            import json
            import os
            from datetime import datetime
            
            # Create queries directory if it doesn't exist
            queries_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries')
            os.makedirs(queries_dir, exist_ok=True)
            
            # Create query data
            query_data = {
                'name': query_name,
                'content': content,
                'created_at': datetime.now().isoformat(),
                'modified_at': datetime.now().isoformat()
            }
            
            # Save to JSON file
            file_path = os.path.join(queries_dir, f"{query_name}.json")
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(query_data, file, indent=2, ensure_ascii=False)
            
            self.log_message(f"Saved query '{query_name}' to JSON file")
            return True
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save query: {str(e)}")
            return False
    
    def rename_current_query(self):
        """Rename the current query tab"""
        current_name = self.get_current_tab_name()
        
        new_name, ok = QInputDialog.getText(
            self,
            "Rename Query",
            "Enter new name for the query:",
            text=current_name
        )
        
        if ok and new_name.strip():
            self.set_current_tab_name(new_name.strip())
            self.log_message(f"Renamed query tab to: {new_name.strip()}")
    
    def show_tab_context_menu(self, position):
        """Show context menu for query tabs"""
        tab_index = self.query_tabs.tabBar().tabAt(position)
        if tab_index >= 0:
            context_menu = QMenu(self)
            
            # Rename action
            rename_action = context_menu.addAction("Rename Query")
            rename_action.triggered.connect(lambda: self.rename_tab_at_index(tab_index))
            
            # Close action (only if more than one tab)
            if self.query_tabs.count() > 1:
                close_action = context_menu.addAction("Close Query")
                close_action.triggered.connect(lambda: self.close_query_tab(tab_index))
            
            context_menu.exec(self.query_tabs.mapToGlobal(position))
    
    def rename_tab_at_index(self, index):
        """Rename a specific tab by index"""
        current_name = self.query_tabs.tabText(index)
        
        new_name, ok = QInputDialog.getText(
            self,
            "Rename Query",
            "Enter new name for the query:",
            text=current_name
        )
        
        if ok and new_name.strip():
            self.query_tabs.setTabText(index, new_name.strip())
            self.log_message(f"Renamed query tab to: {new_name.strip()}")
        
    def setup_menu_bar(self):
        """Setup the application menu bar"""
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu('File')
        
        # Load CSV
        load_csv_action = QAction('Load CSV File', self)
        load_csv_action.triggered.connect(lambda: self.load_file('csv'))
        file_menu.addAction(load_csv_action)
        
        # Load Excel
        load_excel_action = QAction('Load Excel File', self)
        load_excel_action.triggered.connect(lambda: self.load_file('excel'))
        file_menu.addAction(load_excel_action)
        
        # Load JSON
        load_json_action = QAction('Load JSON File', self)
        load_json_action.triggered.connect(lambda: self.load_file('json'))
        file_menu.addAction(load_json_action)
        
        # Load Parquet
        load_parquet_action = QAction('Load Parquet File', self)
        load_parquet_action.triggered.connect(lambda: self.load_file('parquet'))
        file_menu.addAction(load_parquet_action)
        
        file_menu.addSeparator()
        
        # Load Folder (Excel files)
        load_folder_action = QAction('Load Folder (Excel Files)', self)
        load_folder_action.triggered.connect(self.load_folder)
        file_menu.addAction(load_folder_action)
        
        # Load Folder (CSV files)
        load_csv_folder_action = QAction('Load Folder (CSV Files)', self)
        load_csv_folder_action.triggered.connect(self.load_csv_folder)
        file_menu.addAction(load_csv_folder_action)
        
        file_menu.addSeparator()
        
        # Exit
        exit_action = QAction('Exit', self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Query menu
        query_menu = menubar.addMenu('Query')
        
        # Execute query
        execute_action = QAction('Execute Query', self)
        execute_action.setShortcut('F5')
        execute_action.triggered.connect(self.execute_query)
        query_menu.addAction(execute_action)
        
        # Execute selected query
        execute_selected_action = QAction('Execute Selected Query', self)
        execute_selected_action.setShortcut('Ctrl+F5')
        execute_selected_action.triggered.connect(self.execute_selected_query)
        query_menu.addAction(execute_selected_action)
        
        # Clear results
        clear_action = QAction('Clear Results', self)
        clear_action.triggered.connect(self.clear_results)
        query_menu.addAction(clear_action)
        
        query_menu.addSeparator()
        
        # Export Results as Excel
        export_excel_action = QAction('Export Results as Excel...', self)
        export_excel_action.setShortcut('Ctrl+E')
        export_excel_action.triggered.connect(self.export_results_excel)
        query_menu.addAction(export_excel_action)
        
        # PivotJS Visualization
        pivot_action = QAction('Visualize with PivotJS...', self)
        pivot_action.setShortcut('Ctrl+P')
        pivot_action.triggered.connect(self.open_pivot_visualization)
        query_menu.addAction(pivot_action)
        
        # Export Results as CSV
        export_csv_action = QAction('Export Results as CSV...', self)
        export_csv_action.setShortcut('Ctrl+Shift+C')
        export_csv_action.triggered.connect(self.export_results_csv)
        query_menu.addAction(export_csv_action)
        
        # Export Results as JSON
        export_json_action = QAction('Export Results as JSON...', self)
        export_json_action.setShortcut('Ctrl+Shift+J')
        export_json_action.triggered.connect(self.export_results_json)
        query_menu.addAction(export_json_action)
        
        # Export Results as Parquet
        export_parquet_action = QAction('Export Results as Parquet...', self)
        export_parquet_action.setShortcut('Ctrl+Shift+P')
        export_parquet_action.triggered.connect(self.export_results_parquet)
        query_menu.addAction(export_parquet_action)
        
        query_menu.addSeparator()
        
        # New Query
        new_query_action = QAction('New Query', self)
        new_query_action.setShortcut('Ctrl+N')
        new_query_action.triggered.connect(self.new_query)
        query_menu.addAction(new_query_action)
        
        # Open Query
        open_query_action = QAction('Open Query...', self)
        open_query_action.setShortcut('Ctrl+O')
        open_query_action.triggered.connect(self.open_query)
        query_menu.addAction(open_query_action)
        
        # Save Query As
        save_query_as_action = QAction('Save Query As...', self)
        save_query_as_action.setShortcut('Ctrl+Shift+S')
        save_query_as_action.triggered.connect(self.save_query_as)
        query_menu.addAction(save_query_as_action)
        
        query_menu.addSeparator()
        
        # Rename Query
        rename_query_action = QAction('Rename Query...', self)
        rename_query_action.triggered.connect(self.rename_current_query)
        query_menu.addAction(rename_query_action)
        
        # Database menu
        database_menu = menubar.addMenu('Database')
        
        # Add connection
        add_connection_action = QAction('Add Connection...', self)
        add_connection_action.triggered.connect(self.add_database_connection)
        database_menu.addAction(add_connection_action)
        
        # Manage connections
        manage_connections_action = QAction('Manage Connections...', self)
        manage_connections_action.triggered.connect(self.manage_database_connections)
        database_menu.addAction(manage_connections_action)
        
        database_menu.addSeparator()
        
        # Connect/Disconnect submenu
        self.connection_menu = database_menu.addMenu('Connect')
        self.update_connection_menu()
        
        # View menu
        view_menu = menubar.addMenu('View')
        
        # Theme submenu
        theme_menu = view_menu.addMenu('Theme')
        
        # Add theme actions
        for theme_name in self.theme_manager.get_themes():
            theme_action = QAction(theme_name.title(), self)
            theme_action.triggered.connect(lambda checked, name=theme_name: self.apply_theme(name))
            theme_menu.addAction(theme_action)
        
        # Help menu
        help_menu = menubar.addMenu('Help')
        about_action = QAction('About', self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def setup_toolbar(self):
        """Setup the application toolbar"""
        toolbar = self.addToolBar('Main')
        toolbar.setMovable(False)
        
        # Execute query button
        execute_btn = QPushButton('Execute (F5)')
        execute_btn.clicked.connect(self.execute_query)
        toolbar.addWidget(execute_btn)
        
        # New Query button
        new_query_btn = QPushButton('New Query')
        new_query_btn.clicked.connect(self.new_query)
        toolbar.addWidget(new_query_btn)
        
        toolbar.addSeparator()
        
        # Load file buttons
        load_csv_btn = QPushButton('Load CSV')
        load_csv_btn.clicked.connect(lambda: self.load_file('csv'))
        toolbar.addWidget(load_csv_btn)
        
        load_excel_btn = QPushButton('Load Excel')
        load_excel_btn.clicked.connect(lambda: self.load_file('excel'))
        toolbar.addWidget(load_excel_btn)
        
        load_json_btn = QPushButton('Load JSON')
        load_json_btn.clicked.connect(lambda: self.load_file('json'))
        toolbar.addWidget(load_json_btn)
        
        load_parquet_btn = QPushButton('Load Parquet')
        load_parquet_btn.clicked.connect(lambda: self.load_file('parquet'))
        toolbar.addWidget(load_parquet_btn)
        
        toolbar.addSeparator()
        
        load_folder_btn = QPushButton('Load Folder (Excel)')
        load_folder_btn.clicked.connect(self.load_folder)
        toolbar.addWidget(load_folder_btn)
        
        load_csv_folder_btn = QPushButton('Load Folder (CSV)')
        load_csv_folder_btn.clicked.connect(self.load_csv_folder)
        toolbar.addWidget(load_csv_folder_btn)
        
        toolbar.addSeparator()
        
        # PivotJS Visualization button
        pivot_btn = QPushButton('Visualize with PivotJS')
        pivot_btn.clicked.connect(self.open_pivot_visualization)
        pivot_btn.setToolTip('Open current query results in PivotJS for interactive visualization')
        toolbar.addWidget(pivot_btn)
        
    def setup_status_bar(self):
        """Setup the status bar"""
        self.status_bar = self.statusBar()
        
        # Connection status
        self.connection_label = QLabel("Connected to DuckDB (in-memory)")
        self.status_bar.addWidget(self.connection_label)
        
        # Database context
        self.db_context_label = QLabel(f"Database: {self.current_database}")
        self.status_bar.addWidget(self.db_context_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.status_bar.addPermanentWidget(self.progress_bar)
        
        # Query stats
        self.query_stats_label = QLabel("Ready")
        self.status_bar.addPermanentWidget(self.query_stats_label)
        
    def update_database_context_display(self):
        """Update the database context display in the status bar"""
        if self.current_connection == 'local':
            self.db_context_label.setText(f"Database: {self.current_database}")
        else:
            self.db_context_label.setText(f"Database: {self.current_connection}.{self.current_database}")
        
    def load_file(self, file_type: str):
        """Load a file into DuckDB"""
        file_filters = {
            'csv': 'CSV Files (*.csv);;All Files (*)',
            'excel': 'Excel Files (*.xlsx *.xls);;All Files (*)',
            'json': 'JSON Files (*.json *.jsonl);;All Files (*)',
            'parquet': 'Parquet Files (*.parquet);;All Files (*)'
        }
        
        file_path, _ = QFileDialog.getOpenFileName(
            self, f'Load {file_type.upper()} File', '', file_filters.get(file_type, 'All Files (*)')
        )
        
        if not file_path:
            return
            
        try:
            base_table_name = Path(file_path).stem.replace(' ', '_').replace('-', '_')
            table_name = self.get_unique_table_name(base_table_name)
            
            if file_type == 'csv':
                self.load_csv_file_with_dialog(file_path, table_name)
            elif file_type == 'excel':
                self.load_excel_file_with_dialog(file_path, table_name)
            elif file_type == 'json':
                self.load_json_file(file_path, table_name)
            elif file_type == 'parquet':
                self.load_parquet_file(file_path, table_name)
                
            if file_type not in ['csv', 'excel']:  # CSV and Excel loading handle their own success messages
                self.log_message(f"Successfully loaded {file_path} as table '{table_name}'")
                self.refresh_database_tree()
            
        except Exception as e:
            error_msg = f"Error loading {file_path}: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Load Error", error_msg)
            
    def get_unique_table_name(self, base_name: str) -> str:
        """Generate a unique table name by checking existing tables"""
        try:
            # Get existing table names
            existing_tables = set()
            tables_result = self.connection.execute("SHOW TABLES").fetchall()
            for table_row in tables_result:
                existing_tables.add(table_row[0].lower())
            
            # If base name doesn't exist, use it
            if base_name.lower() not in existing_tables:
                return base_name
            
            # Generate unique name with counter
            counter = 1
            while f"{base_name}_{counter}".lower() in existing_tables:
                counter += 1
            
            return f"{base_name}_{counter}"
            
        except Exception:
            # If there's an error checking tables, just return base name
            return base_name
    
    def load_csv_file_with_dialog(self, file_path: str, table_name: str):
        """Load CSV file with configuration dialog"""
        dialog = CSVImportDialog(self, file_path)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            try:
                query = dialog.get_csv_query(table_name)
                self.connection.execute(query)
                self.log_message(f"Successfully loaded {file_path} as table '{table_name}'")
                self.refresh_database_tree()
            except Exception as e:
                # If there's a conversion error, try loading all columns as text
                self.log_message(f"Initial load failed: {str(e)}. Retrying with all columns as text...")
                try:
                    query_with_text = dialog.get_csv_query_as_text(table_name)
                    self.connection.execute(query_with_text)
                    self.log_message(f"Successfully loaded {file_path} as table '{table_name}' with all columns as text")
                    self.refresh_database_tree()
                except Exception as e2:
                    error_msg = f"Error loading CSV file even with text columns: {str(e2)}"
                    self.log_message(error_msg)
                    QMessageBox.critical(self, "CSV Load Error", error_msg)
                
    def load_csv_file(self, file_path: str, table_name: str):
        """Load CSV file using DuckDB (direct method without dialog)"""
        try:
            query = f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{file_path}')"
            self.connection.execute(query)
        except Exception as e:
            # If there's a conversion error, try loading all columns as text
            self.log_message(f"Initial auto-load failed: {str(e)}. Retrying with all columns as text...")
            try:
                query_with_text = f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{file_path}', ALL_VARCHAR=true)"
                self.connection.execute(query_with_text)
                self.log_message(f"Successfully loaded {file_path} as table '{table_name}' with all columns as text")
            except Exception as e2:
                # Re-raise the original error if text loading also fails
                raise e
        
    def load_excel_file_with_dialog(self, file_path: str, table_name: str):
        """Load Excel file with configuration dialog"""
        dialog = ExcelImportDialog(self, file_path)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            try:
                sheet_name = dialog.sheet_input.text().strip() or None
                convert_to_text = dialog.convert_to_text_checkbox.isChecked()
                
                # Use Polars to read Excel file with specified sheet
                if sheet_name:
                    df = pl.read_excel(file_path, sheet_name=sheet_name)
                else:
                    df = pl.read_excel(file_path)  # First sheet by default
                
                # Convert all columns to text if requested
                if convert_to_text:
                    text_columns = []
                    for col in df.columns:
                        text_columns.append(pl.col(col).cast(pl.Utf8).alias(col))
                    df = df.select(text_columns)
                    self.log_message(f"All columns converted to text as requested")
                
                # Convert to DuckDB table
                self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM df")
                success_msg = f"Successfully loaded {file_path} as table '{table_name}'"
                if convert_to_text:
                    success_msg += " (all columns as text)"
                self.log_message(success_msg)
                self.refresh_database_tree()
            except Exception as e:
                error_msg = f"Error loading Excel file: {str(e)}"
                self.log_message(error_msg)
                QMessageBox.critical(self, "Excel Load Error", error_msg)
                
    def load_excel_file(self, file_path: str, table_name: str):
        """Load Excel file using Polars and DuckDB (direct method without dialog)"""
        # Use Polars to read Excel file
        df = pl.read_excel(file_path)
        
        # Convert to DuckDB table
        self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM df")
        
    def load_json_file(self, file_path: str, table_name: str):
        """Load JSON file using DuckDB"""
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{file_path}')"
        self.connection.execute(query)
        
    def load_parquet_file(self, file_path: str, table_name: str):
        """Load Parquet file using DuckDB"""
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
        self.connection.execute(query)
    
    def load_csv_file_with_delimiter(self, file_path: str, table_name: str, delimiter: str = ','):
        """Load CSV file with specified delimiter"""
        try:
            query = f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{file_path}', delim='{delimiter}')"
            self.connection.execute(query)
        except Exception as e:
            # If there's a conversion error, try loading all columns as text
            self.log_message(f"Initial auto-load failed: {str(e)}. Retrying with all columns as text...")
            try:
                query_with_text = f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{file_path}', delim='{delimiter}', ALL_VARCHAR=true)"
                self.connection.execute(query_with_text)
                self.log_message(f"Successfully loaded {file_path} as table '{table_name}' with all columns as text")
            except Exception as e2:
                # Re-raise the original error if text loading also fails
                raise e
    
    def load_excel_file_with_sheet(self, file_path: str, table_name: str, sheet_name: str = None):
        """Load Excel file with specified sheet name"""
        # Use Polars to read Excel file with specified sheet
        if sheet_name:
            df = pl.read_excel(file_path, sheet_name=sheet_name)
        else:
            df = pl.read_excel(file_path)  # First sheet by default
        
        # Convert to DuckDB table
        self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM df")
    
    def load_csv_folder_with_delimiter(self, folder_path: str, table_name: str, delimiter: str = ',', quote_char: str = None, has_header: bool = True, encoding: str = 'utf8'):
        """Load all CSV files from a folder with specified delimiter for automation"""
        if not POLARS_AVAILABLE:
            raise Exception("Polars library is required for folder loading feature")
        
        try:
            import os
            import glob
            
            # Find all CSV files in the folder
            csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
                
            if not csv_files:
                raise Exception("No CSV files found in the selected folder")
            
            # Make sure table name is unique
            table_name = self.get_unique_table_name(table_name)
            
            # Load and combine all CSV files
            combined_df = None
            loaded_files = []
            
            for file_path in csv_files:
                try:
                    # Read CSV with Polars using the configured settings
                    read_options = {
                        'has_header': has_header,
                        'encoding': encoding
                    }
                    
                    # Add separator if specified
                    if delimiter is not None:
                        read_options['separator'] = delimiter
                    
                    # Add quote character if specified
                    if quote_char is not None:
                        read_options['quote_char'] = quote_char
                    
                    df = pl.read_csv(file_path, **read_options)
                    
                    # Add source file column
                    df = df.with_columns(pl.lit(os.path.basename(file_path)).alias('_source_file'))
                    
                    if combined_df is None:
                        combined_df = df
                    else:
                        # Use concat with how='diagonal' to handle different schemas
                        combined_df = pl.concat([combined_df, df], how='diagonal')
                    
                    loaded_files.append(os.path.basename(file_path))
                    
                except Exception as e:
                    self.log_message(f"Warning: Could not load {os.path.basename(file_path)}: {str(e)}")
                    continue
            
            if combined_df is None or combined_df.height == 0:
                raise Exception("No data could be loaded from any CSV files in the folder")
                
            # Create table in DuckDB
            self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM combined_df")
            
            # Log success message
            success_msg = f"Successfully loaded {len(loaded_files)} CSV files into table '{table_name}': {', '.join(loaded_files)}"
            self.log_message(success_msg)
            self.refresh_database_tree()
            
        except Exception as e:
            error_msg = f"Error loading CSV folder: {str(e)}"
            self.log_message(error_msg)
            raise Exception(error_msg)
        
    def load_folder(self):
        """Load all Excel files from a selected folder"""
        if not POLARS_AVAILABLE:
            QMessageBox.critical(
                self, "Polars Required", 
                "Polars library is required for folder loading feature.\n"
                "Please install it with: pip install polars"
            )
            return
            
        folder_path = QFileDialog.getExistingDirectory(
            self, 'Select Folder with Excel Files', ''
        )
        
        if not folder_path:
            return
            
        try:
            import os
            import glob
            
            # Find all Excel files in the folder
            excel_extensions = ['*.xlsx', '*.xls']
            excel_files = []
            
            for ext in excel_extensions:
                excel_files.extend(glob.glob(os.path.join(folder_path, ext)))
                
            if not excel_files:
                QMessageBox.information(
                    self, "No Excel Files", 
                    "No Excel files found in the selected folder."
                )
                return
                
            # Ask for sheet name
            sheet_name, ok = QInputDialog.getText(
                self, 
                "Sheet Name", 
                f"Enter sheet name to load from all {len(excel_files)} Excel files (leave empty for first sheet):",
                text=""
            )
            
            if not ok:
                return
                
            # Use first sheet if empty
            if not sheet_name.strip():
                sheet_name = None  # Polars will use first sheet
            
            # Ask for table name
            table_name, ok = QInputDialog.getText(
                self, 
                "Table Name", 
                f"Enter table name for combined data from {len(excel_files)} Excel files:",
                text="combined_excel_data"
            )
            
            if not ok or not table_name:
                return
                
            # Make sure table name is unique
            table_name = self.get_unique_table_name(table_name)
            
            # Load and combine all Excel files with automatic text conversion for schema conflicts
            combined_df = None
            loaded_files = []
            schema_conflicts_detected = False
            
            for file_path in excel_files:
                try:
                    # Read Excel file with Polars, using specified sheet
                    if sheet_name:
                        df = pl.read_excel(file_path, sheet_name=sheet_name)
                    else:
                        df = pl.read_excel(file_path)  # Use first sheet
                    
                    # Convert all columns to text to avoid schema conflicts
                    # This ensures consistent data types across all files
                    text_columns = []
                    for col in df.columns:
                        text_columns.append(pl.col(col).cast(pl.Utf8).alias(col))
                    
                    df = df.select(text_columns)
                    
                    # Add source file column
                    df = df.with_columns(
                        pl.lit(os.path.basename(file_path)).alias('_source_file')
                    )
                    
                    if combined_df is None:
                        combined_df = df
                    else:
                        try:
                            # Try to concatenate normally first
                            combined_df = pl.concat([combined_df, df], how='vertical')
                        except Exception:
                            # If vertical concat fails, use diagonal (handles different column sets)
                            combined_df = pl.concat([combined_df, df], how='diagonal')
                            schema_conflicts_detected = True
                    
                    loaded_files.append(os.path.basename(file_path))
                    
                except Exception as e:
                    self.log_message(f"Warning: Could not load {os.path.basename(file_path)} (sheet: {sheet_name or 'first'}): {str(e)}")
                    continue
            
            if combined_df is None or combined_df.height == 0:
                QMessageBox.warning(
                    self, "Load Error", 
                    "No data could be loaded from any Excel files in the folder."
                )
                return
                
            # Create table in DuckDB
            self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM combined_df")
            
            # Log success message with information about text conversion
            success_msg = f"Successfully loaded {len(loaded_files)} Excel files into table '{table_name}': {', '.join(loaded_files)}"
            if schema_conflicts_detected:
                success_msg += "\nNote: Different column schemas detected - all columns converted to text for compatibility."
            else:
                success_msg += "\nNote: All columns automatically converted to text to ensure data consistency."
            
            self.log_message(success_msg)
            self.refresh_database_tree()
            
        except Exception as e:
            error_msg = f"Error loading folder: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Load Error", error_msg)
    
    def load_excel_folder(self, folder_path: str, table_name: str, sheet_name: str = None):
        """Load all Excel files from a specified folder for automation"""
        if not POLARS_AVAILABLE:
            raise Exception("Polars library is required for folder loading feature")
            
        try:
            import os
            import glob
            
            # Find all Excel files in the folder
            excel_extensions = ['*.xlsx', '*.xls']
            excel_files = []
            
            for ext in excel_extensions:
                excel_files.extend(glob.glob(os.path.join(folder_path, ext)))
                
            if not excel_files:
                raise Exception("No Excel files found in the selected folder")
            
            # Make sure table name is unique
            table_name = self.get_unique_table_name(table_name)
            
            # Load and combine all Excel files with automatic text conversion for schema conflicts
            combined_df = None
            loaded_files = []
            schema_conflicts_detected = False
            
            for file_path in excel_files:
                try:
                    # Read Excel file with Polars, using specified sheet
                    if sheet_name:
                        df = pl.read_excel(file_path, sheet_name=sheet_name)
                    else:
                        df = pl.read_excel(file_path)  # Use first sheet
                    
                    # Convert all columns to text to avoid schema conflicts
                    # This ensures consistent data types across all files
                    text_columns = []
                    for col in df.columns:
                        text_columns.append(pl.col(col).cast(pl.Utf8).alias(col))
                    
                    df = df.select(text_columns)
                    
                    # Add source file column
                    df = df.with_columns(
                        pl.lit(os.path.basename(file_path)).alias('_source_file')
                    )
                    
                    if combined_df is None:
                        combined_df = df
                    else:
                        try:
                            # Try to concatenate normally first
                            combined_df = pl.concat([combined_df, df], how='vertical')
                        except Exception:
                            # If vertical concat fails, use diagonal (handles different column sets)
                            combined_df = pl.concat([combined_df, df], how='diagonal')
                            schema_conflicts_detected = True
                    
                    loaded_files.append(os.path.basename(file_path))
                    
                except Exception as e:
                    self.log_message(f"Warning: Could not load {os.path.basename(file_path)} (sheet: {sheet_name or 'first'}): {str(e)}")
                    continue
            
            if combined_df is None or combined_df.height == 0:
                raise Exception("No data could be loaded from any Excel files in the folder")
                
            # Create table in DuckDB
            self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM combined_df")
            
            # Log success message with information about text conversion
            success_msg = f"Successfully loaded {len(loaded_files)} Excel files into table '{table_name}': {', '.join(loaded_files)}"
            if schema_conflicts_detected:
                success_msg += "\nNote: Different column schemas detected - all columns converted to text for compatibility."
            else:
                success_msg += "\nNote: All columns automatically converted to text to ensure data consistency."
            
            self.log_message(success_msg)
            self.refresh_database_tree()
            
        except Exception as e:
            error_msg = f"Error loading Excel folder: {str(e)}"
            self.log_message(error_msg)
            raise Exception(error_msg)
    
    def load_csv_folder(self):
        """Load all CSV files from a selected folder"""
        if not POLARS_AVAILABLE:
            QMessageBox.critical(
                self, "Polars Required", 
                "Polars library is required for folder loading feature.\n"
                "Please install it with: pip install polars"
            )
            return
            
        folder_path = QFileDialog.getExistingDirectory(
            self, 'Select Folder with CSV Files', ''
        )
        
        if not folder_path:
            return
            
        try:
            import os
            import glob
            
            # Find all CSV files in the folder
            csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
                
            if not csv_files:
                QMessageBox.information(
                    self, "No CSV Files", 
                    "No CSV files found in the selected folder."
                )
                return
            
            # Use the first CSV file to configure import settings
            first_file = csv_files[0]
            dialog = CSVImportDialog(self, first_file)
            
            if dialog.exec() != QDialog.DialogCode.Accepted:
                return
                
            # Get CSV configuration from dialog
            delimiter = dialog.get_delimiter_value()
            has_header = dialog.header_check.isChecked()
            quote_char = dialog.get_quote_value()
            encoding = dialog.encoding_combo.currentText()
            
            # Ask for table name
            table_name, ok = QInputDialog.getText(
                self, 
                "Table Name", 
                f"Enter table name for combined data from {len(csv_files)} CSV files:",
                text=f"combined_csv_data"
            )
            
            if not ok or not table_name.strip():
                return
                
            table_name = table_name.strip()
            table_name = self.get_unique_table_name(table_name)
            
            # Load and combine all CSV files
            combined_df = None
            loaded_files = []
            
            for file_path in csv_files:
                try:
                    # Read CSV with Polars using the configured settings
                    read_options = {
                        'has_header': has_header,
                        'encoding': encoding if encoding != 'Auto' else 'utf8'
                    }
                    
                    # Only add separator if not using automatic detection
                    if delimiter is not None:
                        read_options['separator'] = delimiter
                    
                    # Handle quote character
                    if quote_char is not None and quote_char != 'Auto':
                        if quote_char == '':
                            read_options['quote_char'] = None
                        else:
                            read_options['quote_char'] = quote_char
                    
                    df = pl.read_csv(file_path, **read_options)
                    
                    # Add source file column
                    df = df.with_columns(pl.lit(os.path.basename(file_path)).alias('_source_file'))
                    
                    if combined_df is None:
                        combined_df = df
                    else:
                        # Use concat with how='diagonal' to handle different schemas
                        combined_df = pl.concat([combined_df, df], how='diagonal')
                    
                    loaded_files.append(os.path.basename(file_path))
                    
                except Exception as e:
                    self.log_message(f"Warning: Could not load {os.path.basename(file_path)}: {str(e)}")
                    continue
            
            if combined_df is None or combined_df.height == 0:
                QMessageBox.warning(
                    self, "Load Error", 
                    "No data could be loaded from any CSV files in the folder."
                )
                return
                
            # Create table in DuckDB
            self.connection.execute(f"CREATE TABLE local.{table_name} AS SELECT * FROM combined_df")
            
            # Log success message
            self.log_message(
                f"Successfully loaded {len(loaded_files)} CSV files into table '{table_name}': {', '.join(loaded_files)}"
            )
            self.refresh_database_tree()
            
        except Exception as e:
            error_msg = f"Error loading CSV folder: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Load Error", error_msg)
        
    def refresh_database_tree(self):
        """Refresh the database tree with current tables"""
        try:
            # Clear existing tables for local database
            self.db_tree.local_tables_node.takeChildren()
            
            # Collect all table names for autocomplete
            all_table_names = []
            
            # Get all tables from the local schema
            tables_result = self.connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'local'").fetchall()
            
            for table_row in tables_result:
                table_name = table_row[0]
                all_table_names.append(f"local.{table_name}")
                all_table_names.append(table_name)  # Also add without schema prefix
                
                # Get column information
                try:
                    columns_result = self.connection.execute(f"DESCRIBE local.{table_name}").fetchall()
                    columns = [f"{col[0]} ({col[1]})" for col in columns_result]
                    self.db_tree.add_table(table_name, columns, 'local')
                except:
                    self.db_tree.add_table(table_name, database='local')
            
            # Refresh connected databases
            connected_dbs = self.connection_manager.get_connected_databases()
            for db_name in connected_dbs:
                try:
                    # Check if this connection has a specific database or connects to server
                    conn = self.connection_manager.connections.get(db_name)
                    if conn and not conn.database:
                        # No specific database - create source node and list all databases
                        if db_name not in self.db_tree.source_nodes:
                            source_item = QTreeWidgetItem(self.db_tree, [f"{db_name} Connection"])
                            self.db_tree.source_nodes[db_name] = source_item
                        
                        try:
                            # Use the attached database connection to show databases
                            # Try different approaches to list databases
                            databases_result = None
                            try:
                                # First try SHOW DATABASES FROM attached_db
                                databases_result = self.connection.execute(f"SHOW DATABASES FROM {db_name}").fetchall()
                            except:
                                try:
                                    # Fallback: Query information_schema directly
                                    databases_result = self.connection.execute(f"SELECT schema_name FROM {db_name}.information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')").fetchall()
                                except:
                                    # Last resort: try to get table schemas
                                    databases_result = self.connection.execute(f"SELECT DISTINCT table_schema FROM {db_name}.information_schema.tables WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')").fetchall()
                            
                            if databases_result:
                                for db_row in databases_result:
                                    schema_name = db_row[0]
                                    # Skip system databases
                                    if schema_name.lower() in ['information_schema', 'mysql', 'performance_schema', 'sys']:
                                        continue
                                    
                                    # Create database node under source
                                    schema_key = f"{db_name}.{schema_name}"
                                    if schema_key not in self.db_tree.database_nodes:
                                        db_item = QTreeWidgetItem(self.db_tree.source_nodes[db_name], [schema_name])
                                        tables_item = QTreeWidgetItem(db_item, ["Tables"])
                                        views_item = QTreeWidgetItem(db_item, ["Views"])
                                        
                                        self.db_tree.database_nodes[schema_key] = db_item
                                        self.db_tree.table_nodes[schema_key] = tables_item
                                        self.db_tree.view_nodes[schema_key] = views_item
                                    
                                    # Get tables for this schema
                                    try:
                                        # Try different approaches to list tables
                                        tables_result = None
                                        try:
                                            # First try SHOW TABLES FROM db.schema
                                            tables_result = self.connection.execute(f"SHOW TABLES FROM {db_name}.{schema_name}").fetchall()
                                        except:
                                            try:
                                                # Fallback: Query information_schema directly
                                                tables_result = self.connection.execute(f"SELECT table_name FROM {db_name}.information_schema.tables WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE'").fetchall()
                                            except:
                                                # Skip if we can't get tables
                                                tables_result = []
                                        
                                        if tables_result:
                                            for table_row in tables_result:
                                                table_name = table_row[0]
                                                # Skip system tables
                                                if table_name.upper().startswith(('INNODB_', 'PERFORMANCE_', 'SYS_')):
                                                    continue
                                                
                                                # Add to autocomplete list
                                                all_table_names.append(f"{db_name}.{schema_name}.{table_name}")
                                                all_table_names.append(f"{schema_name}.{table_name}")
                                                all_table_names.append(table_name)  # Also add without prefixes
                                                
                                                # Create table item directly under Tables node
                                                table_item = QTreeWidgetItem(self.db_tree.table_nodes[schema_key], [table_name])
                                                table_item.setData(0, Qt.ItemDataRole.UserRole, schema_key)
                                        
                                                # Add columns as children of table
                                                try:
                                                    # Try different approaches to get column info
                                                    columns_result = None
                                                    try:
                                                        # First try DESCRIBE
                                                        columns_result = self.connection.execute(f"DESCRIBE {db_name}.{schema_name}.{table_name}").fetchall()
                                                    except:
                                                        try:
                                                            # Fallback: Query information_schema for columns
                                                            columns_result = self.connection.execute(f"SELECT column_name, data_type FROM {db_name}.information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' ORDER BY ordinal_position").fetchall()
                                                        except:
                                                            # Skip columns if we can't get them
                                                            columns_result = []
                                                    
                                                    if columns_result:
                                                        for col in columns_result:
                                                            col_item = QTreeWidgetItem(table_item, [f"{col[0]} ({col[1]})"])
                                                except:
                                                    pass  # No columns info available
                                    except Exception as e:
                                        self.log_message(f"Error getting tables for schema {schema_name}: {e}")
                        except Exception as e:
                            self.log_message(f"Error listing databases for {db_name}: {e}")
                    else:
                        # Specific database connection - create source and database nodes
                        if db_name not in self.db_tree.source_nodes:
                            source_item = QTreeWidgetItem(self.db_tree, [f"{db_name} Connection"])
                            self.db_tree.source_nodes[db_name] = source_item
                        
                        if db_name not in self.db_tree.database_nodes:
                            db_item = QTreeWidgetItem(self.db_tree.source_nodes[db_name], [conn.database or db_name])
                            tables_item = QTreeWidgetItem(db_item, ["Tables"])
                            views_item = QTreeWidgetItem(db_item, ["Views"])
                            
                            self.db_tree.database_nodes[db_name] = db_item
                            self.db_tree.table_nodes[db_name] = tables_item
                            self.db_tree.view_nodes[db_name] = views_item
                        
                        # Get tables for this database
                        try:
                            tables_result = self.connection.execute(f"SHOW TABLES FROM {db_name}").fetchall()
                            table_column = 0  # SHOW TABLES returns table names in first column
                        except:
                            # Fallback to information_schema query - filter out system tables
                            tables_result = self.connection.execute(
                                f"SELECT table_name FROM {db_name}.information_schema.tables "
                                f"WHERE table_schema = '{conn.database or db_name}' AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"
                            ).fetchall()
                            table_column = 0
                        
                        for table_row in tables_result:
                            table_name = table_row[table_column]
                            # Skip system tables that might still appear
                            if table_name.upper().startswith(('INNODB_', 'PERFORMANCE_', 'SYS_')):
                                continue
                            
                            # Add to autocomplete list
                            all_table_names.append(f"{db_name}.{table_name}")
                            all_table_names.append(table_name)  # Also add without database prefix
                            
                            # Create table item directly under Tables node
                            table_item = QTreeWidgetItem(self.db_tree.table_nodes[db_name], [table_name])
                            table_item.setData(0, Qt.ItemDataRole.UserRole, db_name)
                            
                            # Add columns as children of table
                            try:
                                columns_result = self.connection.execute(f"DESCRIBE {db_name}.{conn.database or db_name}.{table_name}").fetchall()
                                for col in columns_result:
                                    col_item = QTreeWidgetItem(table_item, [f"{col[0]} ({col[1]})"])
                            except:
                                pass  # No columns info available
                except Exception as e:
                    self.log_message(f"Error refreshing tables for {db_name}: {e}")
            
            # Store the collected table names for use in new tabs
            self.current_table_names = all_table_names
            
            # Update SQL editor autocomplete with all collected table names for all tabs
            self.update_all_editors_table_names(all_table_names)
                        
        except Exception as e:
            self.log_message(f"Error refreshing database tree: {e}")
            
    def execute_query(self):
        """Execute the SQL query in the editor (selected text if available, otherwise full text)"""
        current_editor = self.get_current_editor()
        if not current_editor:
            self.log_message("No active query editor")
            return
        
        # Check if there's selected text, if so, execute only the selection
        if current_editor.has_selection():
            query = current_editor.get_selected_text().strip()
            self.log_message("Executing selected query...")
        else:
            query = current_editor.get_text().strip()
            self.log_message("Executing full query...")
        
        if not query:
            self.log_message("No query to execute")
            return
        
        # Handle multi-statement queries by splitting on semicolons
        statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
        
        if not statements:
            self.log_message("No valid statements to execute")
            return
        
        # Process USE statements first
        non_use_statements = []
        for stmt in statements:
            if stmt.upper().startswith('USE '):
                try:
                    # Extract database name
                    use_part = stmt[4:].strip()
                    db_name = use_part.strip()
                    
                    # Update the current database context
                    self.current_database = db_name
                    self.update_database_context_display()
                    self.log_message(f"Database context switched to '{db_name}'")
                except Exception as e:
                    self.log_message(f"Error processing USE statement: {e}")
                    return
            else:
                non_use_statements.append(stmt)
        
        # If there are non-USE statements, execute them
        if non_use_statements:
            # Join the remaining statements
            remaining_query = '; '.join(non_use_statements)
            
            # Execute with pagination (default page size and first page)
            self.execute_paginated_query(remaining_query, 0, 1000)
        else:
            # Only USE statements were executed
            self.progress_bar.setVisible(False)
            self.query_stats_label.setText("Ready")
    
    def execute_selected_query(self):
        """Execute only the selected text as a query"""
        current_editor = self.get_current_editor()
        if not current_editor:
            self.log_message("No active query editor")
            return
        
        if not current_editor.has_selection():
            self.log_message("No text selected. Please select the query text you want to execute.")
            return
        
        query = current_editor.get_selected_text().strip()
        
        if not query:
            self.log_message("Selected text is empty")
            return
        
        self.log_message("Executing selected query...")
        
        # Handle multi-statement queries by splitting on semicolons
        statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
        
        if not statements:
            self.log_message("No valid statements in selection")
            return
        
        # Process USE statements first
        non_use_statements = []
        for stmt in statements:
            if stmt.upper().startswith('USE '):
                try:
                    # Extract database name
                    use_part = stmt[4:].strip()
                    db_name = use_part.strip()
                    
                    # Update the current database context
                    self.current_database = db_name
                    self.update_database_context_display()
                    self.log_message(f"Database context switched to '{db_name}'")
                except Exception as e:
                    self.log_message(f"Error processing USE statement: {e}")
                    return
            else:
                non_use_statements.append(stmt)
        
        # If there are non-USE statements, execute them
        if non_use_statements:
            # Join the remaining statements
            remaining_query = '; '.join(non_use_statements)
            
            # Execute with pagination (default page size and first page)
            self.execute_paginated_query(remaining_query, 0, 1000)
        else:
            # Only USE statements were executed
            self.progress_bar.setVisible(False)
            self.query_stats_label.setText("Ready")
        
    def on_query_finished(self, result, query, total_count):
        """Handle successful query completion"""
        data, columns = result
        
        # Get the current query tab index
        current_query_tab = self.query_tabs.currentIndex()
        if current_query_tab in self.query_results_tables:
            # Store results data for this query tab
            current_page = getattr(self.query_worker, 'page_number', 0)
            self.query_results_tables[current_query_tab] = {
                'data': data,
                'columns': columns,
                'total_count': total_count,
                'current_page': current_page,
                'query': query
            }
            
            # Display results in the single results table
            self.single_results_table.display_results(data, columns, total_count, current_page, query)
            
            # Switch to Results tab to show the data
            self.results_tabs.setCurrentIndex(0)
        
        # Update status
        if total_count == -1:
            # Non-paginated results
            row_count = len(data)
            self.query_stats_label.setText(f"Query completed. {row_count} rows returned.")
            self.log_message(f"Query executed successfully. {row_count} rows returned.")
        else:
            # Paginated results
            page_size = getattr(self.query_worker, 'page_size', 1000)
            start_row = current_page * page_size + 1
            end_row = min((current_page + 1) * page_size, total_count)
            self.query_stats_label.setText(f"Query completed. Showing {start_row}-{end_row} of {total_count} rows.")
            self.log_message(f"Query executed successfully. Showing {start_row}-{end_row} of {total_count} total rows.")
        
        # Hide progress
        self.progress_bar.setVisible(False)
        
        # Always refresh database tree to ensure autocomplete is up to date
        # This ensures that all tabs have current table information for autocomplete
        self.refresh_database_tree()
        
    def on_query_error(self, error_msg: str):
        """Handle query execution error"""
        # Check if this is a database context switch message
        if "Database context switched to" in error_msg:
            # Extract database name from the message
            import re
            match = re.search(r"Database context switched to '([^']+)'", error_msg)
            if match:
                new_db = match.group(1)
                self.current_database = new_db
                
                # Find which connection contains this database
                self.current_connection = 'local'  # Default
                for conn_name in self.connection_manager.get_connected_databases():
                    try:
                        # Try to find the database in this connection
                        result = self.connection.execute(f"SELECT schema_name FROM {conn_name}.information_schema.schemata WHERE schema_name = '{new_db}'").fetchall()
                        if result:
                            self.current_connection = conn_name
                            break
                    except:
                        continue
                
                self.update_database_context_display()
                self.log_message(f"Database context switched to '{new_db}' (connection: {self.current_connection})")
                self.query_stats_label.setText(f"Database context: {self.current_connection}.{new_db}")
            else:
                self.log_message(f"Query info: {error_msg}")
                self.query_stats_label.setText("Context switched")
        else:
            self.log_message(f"Query error: {error_msg}")
            self.query_stats_label.setText("Query failed")
            # Switch to messages tab to show error
            self.results_tabs.setCurrentIndex(1)
            
        self.progress_bar.setVisible(False)
        
    def on_query_progress(self, message: str):
        """Handle query progress updates"""
        self.query_stats_label.setText(message)
        
    def execute_paginated_query(self, query, page_number, page_size):
        """Execute a query with pagination"""
        # Show progress
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.query_stats_label.setText("Executing query...")
        
        # Execute query in worker thread with pagination
        self.query_worker = QueryWorker(self.connection, query, self.current_database, self.current_connection, page_size, page_number)
        self.query_worker.finished.connect(self.on_query_finished)
        self.query_worker.error.connect(self.on_query_error)
        self.query_worker.progress.connect(self.on_query_progress)
        self.query_worker.start()

    def clear_results(self):
        """Clear the results table and messages"""
        current_query_index = self.query_tabs.currentIndex()
        if current_query_index in self.query_results_tables:
            self.query_results_tables[current_query_index].clear_results()
        self.messages_text.clear()
        self.query_stats_label.setText("Ready")
        
    def export_results_excel(self):
        """Export query results to Excel file"""
        current_query_index = self.query_tabs.currentIndex()
        if current_query_index not in self.query_results_tables:
            QMessageBox.warning(self, "Warning", "No query results to export.")
            return
            
        if self.single_results_table.table.rowCount() == 0:
            QMessageBox.warning(self, "Warning", "No data to export.")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Results as Excel", "", "Excel Files (*.xlsx);;All Files (*)"
        )
        
        if not file_path:
            return
            
        try:
            import pandas as pd
            
            # Get data from the table
            table = self.single_results_table.table
            rows = table.rowCount()
            cols = table.columnCount()
            
            # Get column headers
            headers = []
            for col in range(cols):
                header_item = table.horizontalHeaderItem(col)
                headers.append(header_item.text() if header_item else f"Column_{col}")
            
            # Get data
            data = []
            for row in range(rows):
                row_data = []
                for col in range(cols):
                    item = table.item(row, col)
                    row_data.append(item.text() if item else "")
                data.append(row_data)
            
            # Create DataFrame and export
            df = pd.DataFrame(data, columns=headers)
            df.to_excel(file_path, index=False, engine='openpyxl')
            
            self.log_message(f"Results exported to Excel: {file_path}")
            QMessageBox.information(self, "Success", f"Results exported successfully to:\n{file_path}")
            
        except ImportError:
            QMessageBox.critical(
                self, "Error", 
                "pandas and openpyxl are required for Excel export.\n"
                "Please install them using:\n"
                "pip install pandas openpyxl"
            )
        except Exception as e:
            error_msg = f"Error exporting to Excel: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Export Error", error_msg)
    
    def export_results_csv(self):
        """Export query results to CSV file"""
        current_query_index = self.query_tabs.currentIndex()
        if current_query_index not in self.query_results_tables:
            QMessageBox.warning(self, "Warning", "No query results to export.")
            return
            
        if self.single_results_table.table.rowCount() == 0:
            QMessageBox.warning(self, "Warning", "No data to export.")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Results as CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        
        if not file_path:
            return
            
        try:
            import csv
            
            # Get data from the table
            table = self.single_results_table.table
            rows = table.rowCount()
            cols = table.columnCount()
            
            # Get column headers
            headers = []
            for col in range(cols):
                header_item = table.horizontalHeaderItem(col)
                headers.append(header_item.text() if header_item else f"Column_{col}")
            
            # Write to CSV file
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write headers
                writer.writerow(headers)
                
                # Write data
                for row in range(rows):
                    row_data = []
                    for col in range(cols):
                        item = table.item(row, col)
                        row_data.append(item.text() if item else "")
                    writer.writerow(row_data)
            
            self.log_message(f"Results exported to CSV: {file_path}")
            QMessageBox.information(self, "Success", f"Results exported successfully to:\n{file_path}")
            
        except Exception as e:
            error_msg = f"Error exporting to CSV: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Export Error", error_msg)
    
    def export_results_json(self):
        """Export query results to JSON file"""
        current_query_index = self.query_tabs.currentIndex()
        if current_query_index not in self.query_results_tables:
            QMessageBox.warning(self, "Warning", "No query results to export.")
            return
            
        if self.single_results_table.table.rowCount() == 0:
            QMessageBox.warning(self, "Warning", "No data to export.")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Results as JSON", "", "JSON Files (*.json);;All Files (*)"
        )
        
        if not file_path:
            return
            
        try:
            import json
            
            # Get data from the table
            table = self.single_results_table.table
            rows = table.rowCount()
            cols = table.columnCount()
            
            # Get column headers
            headers = []
            for col in range(cols):
                header_item = table.horizontalHeaderItem(col)
                headers.append(header_item.text() if header_item else f"Column_{col}")
            
            # Get data as list of dictionaries
            data = []
            for row in range(rows):
                row_dict = {}
                for col in range(cols):
                    item = table.item(row, col)
                    row_dict[headers[col]] = item.text() if item else ""
                data.append(row_dict)
            
            # Write to JSON file
            with open(file_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=2, ensure_ascii=False)
            
            self.log_message(f"Results exported to JSON: {file_path}")
            QMessageBox.information(self, "Success", f"Results exported successfully to:\n{file_path}")
            
        except Exception as e:
            error_msg = f"Error exporting to JSON: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Export Error", error_msg)
    
    def export_results_parquet(self):
        """Export query results to Parquet file"""
        current_query_index = self.query_tabs.currentIndex()
        if current_query_index not in self.query_results_tables:
            QMessageBox.warning(self, "Warning", "No query results to export.")
            return
            
        if self.single_results_table.table.rowCount() == 0:
            QMessageBox.warning(self, "Warning", "No data to export.")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Results as Parquet", "", "Parquet Files (*.parquet);;All Files (*)"
        )
        
        if not file_path:
            return
            
        try:
            import pandas as pd
            
            # Get data from the table
            table = self.single_results_table.table
            rows = table.rowCount()
            cols = table.columnCount()
            
            # Get column headers
            headers = []
            for col in range(cols):
                header_item = table.horizontalHeaderItem(col)
                headers.append(header_item.text() if header_item else f"Column_{col}")
            
            # Get data
            data = []
            for row in range(rows):
                row_data = []
                for col in range(cols):
                    item = table.item(row, col)
                    row_data.append(item.text() if item else "")
                data.append(row_data)
            
            # Create DataFrame and export to Parquet
            df = pd.DataFrame(data, columns=headers)
            df.to_parquet(file_path, index=False, engine='pyarrow')
            
            self.log_message(f"Results exported to Parquet: {file_path}")
            QMessageBox.information(self, "Success", f"Results exported successfully to:\n{file_path}")
            
        except ImportError:
            QMessageBox.critical(
                self, "Error", 
                "pandas and pyarrow are required for Parquet export.\n"
                "Please install them using:\n"
                "pip install pandas pyarrow"
            )
        except Exception as e:
            error_msg = f"Error exporting to Parquet: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "Export Error", error_msg)
        
    def log_message(self, message: str):
        """Add a message to the messages log"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.messages_text.append(f"[{timestamp}] {message}")
        
    def show_about(self):
        """Show about dialog"""
        QMessageBox.about(
            self, 
            "About DuckDB SQL GUI",
            "DuckDB SQL GUI\n\n"
            "A PyQt6-based database interface for DuckDB\n"
            "Supports loading CSV, Excel, JSON, and Parquet files\n\n"
            "Built with:\n"
            "- PyQt6\n"
            "- DuckDB\n"
            "- Polars\n"
            "- QScintilla (optional)"
        )
        
    def add_database_connection(self):
        """Show dialog to add a new database connection"""
        dialog = DatabaseConnectionDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            connection = dialog.get_connection_data()
            self.connection_manager.add_connection(connection)
            self.update_connection_menu()
            self.log_message(f"Added connection '{connection.name}'")
    
    def manage_database_connections(self):
        """Show dialog to manage existing database connections"""
        # This would open a dialog to list, edit, and delete connections
        # For now, just show a simple message
        connections = self.connection_manager.get_connection_names()
        if connections:
            msg = "Existing connections:\n" + "\n".join(connections)
        else:
            msg = "No database connections configured."
        QMessageBox.information(self, "Database Connections", msg)
    
    def update_connection_menu(self):
        """Update the connection menu with available connections"""
        self.connection_menu.clear()
        
        connections = self.connection_manager.get_connection_names()
        if not connections:
            no_conn_action = QAction('No connections configured', self)
            no_conn_action.setEnabled(False)
            self.connection_menu.addAction(no_conn_action)
            return
        
        for conn_name in connections:
            conn = self.connection_manager.connections[conn_name]
            
            # Create submenu for each connection
            conn_submenu = self.connection_menu.addMenu(conn_name)
            
            # Connect/Disconnect action
            if conn.is_connected:
                connect_action = QAction('Disconnect', self)
                connect_action.triggered.connect(lambda checked, name=conn_name: self.disconnect_database(name))
            else:
                connect_action = QAction('Connect', self)
                connect_action.triggered.connect(lambda checked, name=conn_name: self.connect_database(name))
            conn_submenu.addAction(connect_action)
            
            conn_submenu.addSeparator()
            
            # Edit action
            edit_action = QAction('Edit...', self)
            edit_action.triggered.connect(lambda checked, name=conn_name: self.edit_database_connection(name))
            conn_submenu.addAction(edit_action)
            
            # Delete action
            delete_action = QAction('Delete', self)
            delete_action.triggered.connect(lambda checked, name=conn_name: self.delete_database_connection(name))
            conn_submenu.addAction(delete_action)
    
    def connect_database(self, connection_name: str):
        """Connect to a database"""
        if self.connection_manager.connect_database(connection_name):
            self.log_message(f"Connected to {connection_name}")
            
            # Update current connection context
            self.current_connection = connection_name
            
            # Get the default database for this connection
            conn_info = self.connection_manager.connections.get(connection_name)
            if conn_info and conn_info.database:
                self.current_database = conn_info.database
            else:
                # Try to get the first available database
                try:
                    databases = self.connection.execute(f"SELECT schema_name FROM {connection_name}.information_schema.schemata LIMIT 1").fetchall()
                    if databases:
                        self.current_database = databases[0][0]
                    else:
                        self.current_database = 'main'  # fallback
                except:
                    self.current_database = 'main'  # fallback
            
            self.update_database_context_display()
            self.update_connection_menu()
            self.refresh_database_tree()
        else:
            self.log_message(f"Failed to connect to {connection_name}")
            QMessageBox.warning(self, "Connection Error", f"Failed to connect to {connection_name}")
    
    def disconnect_database(self, connection_name: str):
        """Disconnect from a database"""
        self.connection_manager.disconnect_database(connection_name)
        self.log_message(f"Disconnected from {connection_name}")
        
        # Reset connection context to local if we were using this connection
        if self.current_connection == connection_name:
            self.current_connection = 'local'
            self.current_database = 'local'
            self.update_database_context_display()
        
        self.update_connection_menu()
        # Remove the database from the tree
        self.db_tree.remove_database(connection_name)
        self.refresh_database_tree()
    
    def edit_database_connection(self, connection_name: str):
        """Edit an existing database connection"""
        # Get the existing connection
        conn = self.connection_manager.connections.get(connection_name)
        if not conn:
            QMessageBox.warning(self, "Error", f"Connection '{connection_name}' not found")
            return
        
        # Open the connection dialog with existing data
        dialog = DatabaseConnectionDialog(self, conn)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Get the updated connection data
            conn_data = dialog.get_connection_data()
            
            # If the name changed, we need to handle it specially
            new_name = conn_data.name
            if new_name != connection_name:
                # Check if new name already exists
                if new_name in self.connection_manager.connections:
                    QMessageBox.warning(self, "Error", f"Connection '{new_name}' already exists")
                    return
                
                # Remove old connection and add new one
                self.connection_manager.remove_connection(connection_name)
                self.connection_manager.add_connection(
                    conn_data.name, conn_data.host, conn_data.port,
                    conn_data.database, conn_data.username, conn_data.password,
                    conn_data.ssl_cert, conn_data.ssl_key, conn_data.ssl_ca,
                    conn.db_type
                )
            else:
                # Update existing connection
                conn.host = conn_data.host
                conn.port = conn_data.port
                conn.database = conn_data.database
                conn.username = conn_data.username
                conn.password = conn_data.password
                conn.ssl_cert = conn_data.ssl_cert
                conn.ssl_key = conn_data.ssl_key
                conn.ssl_ca = conn_data.ssl_ca
            
            # Save connections and update UI
            self.connection_manager.save_connections()
            self.update_connection_menu()
            self.log_message(f"Updated connection '{new_name}'")
    
    def delete_database_connection(self, connection_name: str):
        """Delete a database connection"""
        # Confirm deletion
        reply = QMessageBox.question(
            self, "Confirm Deletion",
            f"Are you sure you want to delete the connection '{connection_name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Disconnect if connected
            conn = self.connection_manager.connections.get(connection_name)
            if conn and conn.is_connected:
                self.disconnect_database(connection_name)
            
            # Remove the connection
            self.connection_manager.remove_connection(connection_name)
            self.connection_manager.save_connections()
            self.update_connection_menu()
            self.log_message(f"Deleted connection '{connection_name}'")
    
    def apply_theme(self, theme_name: str):
        """Apply a theme to the application"""
        stylesheet = self.theme_manager.get_theme_stylesheet(theme_name)
        self.setStyleSheet(stylesheet)
        self.theme_manager.set_theme(theme_name)
        
        # Apply theme to all SQL editors
        if hasattr(self, 'query_tabs'):
            self.apply_theme_to_all_editors(theme_name)
        
        self.log_message(f"Applied {theme_name.title()} theme")
    
    def closeEvent(self, event):
        """Handle application close event"""
        # Close database connection
        if hasattr(self, 'connection') and self.connection:
            try:
                self.connection.close()
                print("Database connection closed")
            except Exception as e:
                print(f"Error closing database connection: {e}")
        
        # Clean up temporary database file
        try:
            import os
            app_dir = os.path.dirname(os.path.abspath(__file__))
            temp_dir = os.path.join(app_dir, 'temp')
            db_file = os.path.join(temp_dir, 'duckdb_gui_temp.duckdb')
            wal_file = os.path.join(temp_dir, 'duckdb_gui_temp.duckdb.wal')
            
            # Remove database files if they exist
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"Temporary database file deleted: {db_file}")
            
            if os.path.exists(wal_file):
                os.remove(wal_file)
                print(f"Temporary WAL file deleted: {wal_file}")
            
            # Remove temp directory if empty
            if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                os.rmdir(temp_dir)
                print("Temporary directory removed")
                
        except Exception as e:
            print(f"Error cleaning up temporary files: {e}")
        
        # Save settings
        try:
            settings = QSettings()
            settings.sync()
        except Exception as e:
            print(f"Error saving settings: {e}")
        
        event.accept()
    
    def setup_web_server(self):
        """Initialize Flask web server for PivotJS visualization"""
        self.flask_app = Flask(__name__, static_folder='static')
        self.web_server_port = 5000
        
        # Configure Flask routes
        @self.flask_app.route('/pivot')
        def pivot_page():
            return send_from_directory('static', 'pivot.html')
        
        @self.flask_app.route('/api/pivot-data')
        def get_pivot_data():
            try:
                tab_id = request.args.get('tab_id')
                if not tab_id:
                    return jsonify({'success': False, 'error': 'No tab_id provided'})
                
                try:
                    tab_index = int(tab_id)
                except ValueError:
                    return jsonify({'success': False, 'error': 'Invalid tab_id format'})
                
                # Get results data for the specified tab
                if tab_index not in self.query_results_tables:
                    return jsonify({'success': False, 'error': 'Tab not found'})
                
                results_data = self.query_results_tables[tab_index]
                
                if not results_data['data'] or not results_data['columns']:
                    return jsonify({'success': False, 'error': 'No data available for this tab'})
                
                # Convert data to list of dictionaries for PivotJS
                pivot_data = []
                for row in results_data['data']:
                    row_dict = {}
                    for i, col_name in enumerate(results_data['columns']):
                        if i < len(row):
                            row_dict[col_name] = row[i]
                        else:
                            row_dict[col_name] = None
                    pivot_data.append(row_dict)
                
                # Get query tab name
                tab_name = self.query_tabs.tabText(tab_index) if tab_index < self.query_tabs.count() else f"Query {tab_index + 1}"
                
                return jsonify({
                    'success': True,
                    'data': pivot_data,
                    'query_info': {
                        'tab_name': tab_name,
                        'query': results_data.get('query', ''),
                        'total_count': results_data.get('total_count', len(pivot_data))
                    }
                })
                
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})
        
        # Start Flask server in a separate thread
        def run_server():
            self.flask_app.run(host='127.0.0.1', port=self.web_server_port, debug=False, use_reloader=False)
        
        self.web_server_thread = threading.Thread(target=run_server, daemon=True)
        self.web_server_thread.start()
        print(f"Web server started on http://127.0.0.1:{self.web_server_port}")
    
    def open_pivot_visualization(self):
        """Open PivotJS visualization for current query tab"""
        current_tab_index = self.query_tabs.currentIndex()
        
        if current_tab_index == -1:
            QMessageBox.warning(self, "Warning", "No query tab selected.")
            return
        
        # Check if current tab has results
        if current_tab_index not in self.query_results_tables:
            QMessageBox.warning(self, "Warning", "No results available for the current query tab.")
            return
        
        results_data = self.query_results_tables[current_tab_index]
        if not results_data['data'] or not results_data['columns']:
            QMessageBox.warning(self, "Warning", "No data available for visualization. Please run a query first.")
            return
        
        # Open browser with PivotJS visualization
        url = f"http://127.0.0.1:{self.web_server_port}/pivot?tab_id={current_tab_index}"
        try:
            webbrowser.open(url)
            self.log_message(f"Opened PivotJS visualization: {url}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to open browser: {str(e)}")
            self.log_message(f"Error opening browser: {str(e)}")


def main():
    """Main application entry point"""
    app = QApplication(sys.argv)
    app.setApplicationName("DuckDB SQL GUI")
    app.setApplicationVersion("1.0.0")
    
    # Create and show main window
    window = DuckDBGUI()
    window.show()
    
    # Start event loop
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
