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
from PyQt6.QtGui import QAction, QIcon, QFont, QPixmap

try:
    from PyQt6.Qsci import QsciScintilla, QsciLexerSQL
    QSCINTILLA_AVAILABLE = True
except ImportError:
    QSCINTILLA_AVAILABLE = False
    print("QScintilla not available, using basic text editor")

import duckdb
import polars as pl


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
            self.parent_gui.sql_editor.set_text(query)
            
    def describe_table(self, table_name: str, database: str = None):
        """Insert DESCRIBE query for table"""
        if self.parent_gui:
            database = database or 'local'
            if database == 'local':
                query = f"DESCRIBE local.{table_name};"
            else:
                query = f"DESCRIBE {database}.{table_name};"
            self.parent_gui.sql_editor.set_text(query)
            
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


class SQLEditor(QWidget):
    """SQL Editor with syntax highlighting"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_theme = 'light'
        self.parent_gui = parent
        self.completer = None
        self.table_names = []
        self.setup_ui()
        self.setup_completer()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        if QSCINTILLA_AVAILABLE:
            self.editor = QsciScintilla()
            self.lexer = QsciLexerSQL()
            self.editor.setLexer(self.lexer)
            self.editor.setAutoIndent(True)
            self.editor.setIndentationsUseTabs(False)
            self.editor.setIndentationWidth(4)
            self.editor.setTabWidth(4)
            self.editor.setAutoCompletionSource(QsciScintilla.AutoCompletionSource.AcsAll)
            self.editor.setAutoCompletionThreshold(1)
            self.editor.setAutoCompletionCaseSensitivity(False)
            
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
        self.completer = QCompleter()
        self.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        
        # Create string list model for table names
        self.model = QStringListModel()
        self.completer.setModel(self.model)
        
        # Set completer for the editor
        if QSCINTILLA_AVAILABLE:
            # For QsciScintilla, we need to handle completion manually
            pass  # QsciScintilla has its own autocompletion system
        else:
            self.editor.setCompleter(self.completer)
    
    def update_table_names(self, table_names: list):
        """Update the list of table names for autocomplete"""
        self.table_names = table_names
        if self.model:
            self.model.setStringList(table_names)
        
        # For QsciScintilla, update the API for autocompletion
        if QSCINTILLA_AVAILABLE and hasattr(self, 'editor'):
            from PyQt6.Qsci import QsciAPIs
            if hasattr(self, 'lexer'):
                # Create API for autocompletion
                self.api = QsciAPIs(self.lexer)
                
                # Add SQL keywords
                sql_keywords = [
                    'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE',
                    'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW',
                    'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON',
                    'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
                    'UNION', 'INTERSECT', 'EXCEPT', 'AS', 'DISTINCT',
                    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'AND', 'OR', 'NOT'
                ]
                
                for keyword in sql_keywords:
                    self.api.add(keyword)
                
                # Add table names
                for table_name in table_names:
                    self.api.add(table_name)
                
                # Prepare the API
                self.api.prepare()
        
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
        
        # Results table
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
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
        
        # Update table
        self.table.setRowCount(len(data))
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)
        
        for row_idx, row_data in enumerate(data):
            for col_idx, cell_data in enumerate(row_data):
                item = QTableWidgetItem(str(cell_data) if cell_data is not None else "")
                self.table.setItem(row_idx, col_idx, item)
                
        self.table.resizeColumnsToContents()
        
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


class DuckDBGUI(QMainWindow):
    """Main application window"""
    
    def __init__(self):
        super().__init__()
        self.connection = None
        self.query_worker = None
        self.current_database = 'local'  # Track current database context
        self.current_connection = 'local'  # Track current connection context
        
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
        
    def setup_database(self):
        """Initialize DuckDB connection"""
        try:
            self.connection = duckdb.connect(':memory:')
            # Create a named database called 'local' for easier referencing
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS local")
            self.connection.execute("USE local")
            print("DuckDB connection established with 'local' database")
        except Exception as e:
            QMessageBox.critical(self, "Database Error", f"Failed to connect to DuckDB: {e}")
            
    def setup_ui(self):
        """Setup the main user interface"""
        self.setWindowTitle("DuckDB SQL GUI")
        self.setGeometry(100, 100, 1400, 800)
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QHBoxLayout(central_widget)
        
        # Create main splitter
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(main_splitter)
        
        # Left panel - Database tree
        self.db_tree = DatabaseTreeWidget(self)
        main_splitter.addWidget(self.db_tree)
        
        # Right panel - SQL editor and results
        right_splitter = QSplitter(Qt.Orientation.Vertical)
        main_splitter.addWidget(right_splitter)
        
        # SQL Editor
        self.sql_editor = SQLEditor(self)
        right_splitter.addWidget(self.sql_editor)
        
        # Results area with tabs
        self.results_tabs = QTabWidget()
        
        # Results table
        self.results_table = ResultsTableWidget()
        self.results_table.parent_gui = self
        self.results_tabs.addTab(self.results_table, "Results")
        
        # Messages/Log area
        self.messages_text = QTextEdit()
        self.messages_text.setMaximumHeight(150)
        self.messages_text.setReadOnly(True)
        self.results_tabs.addTab(self.messages_text, "Messages")
        
        right_splitter.addWidget(self.results_tabs)
        
        # Set splitter proportions
        main_splitter.setSizes([300, 1100])
        right_splitter.setSizes([400, 400])
        
        # Add some sample SQL
        sample_sql = """-- Welcome to DuckDB SQL GUI
-- Load files using the File menu or toolbar
-- Example queries:

SELECT 'Hello, DuckDB!' as message;

-- Show all tables
SHOW TABLES;

-- Describe a table structure
-- DESCRIBE table_name;
"""
        self.sql_editor.set_text(sample_sql)
        
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
        
        # Clear results
        clear_action = QAction('Clear Results', self)
        clear_action.triggered.connect(self.clear_results)
        query_menu.addAction(clear_action)
        
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
                self.load_csv_file(file_path, table_name)
            elif file_type == 'excel':
                self.load_excel_file(file_path, table_name)
            elif file_type == 'json':
                self.load_json_file(file_path, table_name)
            elif file_type == 'parquet':
                self.load_parquet_file(file_path, table_name)
                
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
    
    def load_csv_file(self, file_path: str, table_name: str):
        """Load CSV file using DuckDB"""
        query = f"CREATE TABLE local.{table_name} AS SELECT * FROM read_csv_auto('{file_path}')"
        self.connection.execute(query)
        
    def load_excel_file(self, file_path: str, table_name: str):
        """Load Excel file using Polars and DuckDB"""
        # Use Polars to read Excel file
        df = pl.read_excel(file_path)
        
        # Convert to DuckDB table
        self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
    def load_json_file(self, file_path: str, table_name: str):
        """Load JSON file using DuckDB"""
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{file_path}')"
        self.connection.execute(query)
        
    def load_parquet_file(self, file_path: str, table_name: str):
        """Load Parquet file using DuckDB"""
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
        self.connection.execute(query)
        
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
            
            # Update SQL editor autocomplete with all collected table names
            self.sql_editor.update_table_names(all_table_names)
                    
        except Exception as e:
            self.log_message(f"Error refreshing database tree: {e}")
            
    def execute_query(self):
        """Execute the SQL query in the editor"""
        query = self.sql_editor.get_text().strip()
        
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
        
    def on_query_finished(self, result, query, total_count):
        """Handle successful query completion"""
        data, columns = result
        
        # Display results with pagination info
        current_page = getattr(self.query_worker, 'page_number', 0)
        self.results_table.display_results(data, columns, total_count, current_page, query)
        
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
        
        # Switch to results tab
        self.results_tabs.setCurrentIndex(0)
        
        # Only refresh tree if the query might have created/dropped tables
        query_upper = query.upper().strip()
        if any(keyword in query_upper for keyword in ['CREATE', 'DROP', 'ALTER']):
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
        self.results_table.clear_results()
        self.messages_text.clear()
        self.query_stats_label.setText("Ready")
        
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
        
        # Apply theme to SQL editor
        if hasattr(self, 'sql_editor'):
            self.sql_editor.apply_theme(theme_name)
        
        self.log_message(f"Applied {theme_name.title()} theme")
    
    def closeEvent(self, event):
        """Handle application close event"""
        if self.connection:
            self.connection.close()
        event.accept()


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
