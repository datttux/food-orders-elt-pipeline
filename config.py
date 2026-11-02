"""config.py

Configuration settings for the Batch-Based ELT Pipeline.
Stores file paths, batch processing parameters, and database connection details.
"""

import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", str(BASE_DIR / "food_orders.csv"))
VALIDATION_REPORT_PATH = os.getenv("VALIDATION_REPORT_PATH", str(BASE_DIR / "validation_report.txt"))
REJECTED_RECORDS_PATH = os.getenv("REJECTED_RECORDS_PATH", str(BASE_DIR / "rejected_records.csv"))

# Batch Processing Parameters
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))

# Database Engine: 'mysql' (default) or 'sqlite' (for local testing/fallback)
DB_ENGINE = os.getenv("DB_ENGINE", "mysql").lower()

# MySQL Database Configurations
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

# Database and Table Names
RAW_DB_NAME = os.getenv("RAW_DB_NAME", "food_orders_raw_db")
RAW_TABLE_NAME = "raw_food_orders"

ANALYTICS_DB_NAME = os.getenv("ANALYTICS_DB_NAME", "food_orders_analytics_db")
ANALYTICS_TABLE_NAME = "cleaned_food_orders"

# SQLite File Paths (used if DB_ENGINE == 'sqlite' or fallback)
SQLITE_RAW_DB = str(BASE_DIR / "food_orders_raw.db")
SQLITE_ANALYTICS_DB = str(BASE_DIR / "food_orders_analytics.db")


def get_mysql_connection(database=None):
    """Establishes and returns a connection to MySQL server."""
    import mysql.connector

    config = {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
    }
    if database:
        config["database"] = database
    return mysql.connector.connect(**config)


def get_db_connection(target="raw"):
    """
    Returns an active database connection and active engine type ('mysql' or 'sqlite').
    target: 'raw', 'analytics', or 'server'
    """
    if DB_ENGINE == "sqlite":
        import sqlite3
        db_path = SQLITE_RAW_DB if target in ("raw", "raw_server") else SQLITE_ANALYTICS_DB
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn, "sqlite"

    try:
        if target == "raw":
            return get_mysql_connection(database=RAW_DB_NAME), "mysql"
        elif target == "analytics":
            return get_mysql_connection(database=ANALYTICS_DB_NAME), "mysql"
        else:
            return get_mysql_connection(), "mysql"
    except Exception as e:
        print(f"[config] MySQL connection warning: {e}")
        print(f"[config] Falling back to SQLite backend for execution verification.")
        import sqlite3
        db_path = SQLITE_RAW_DB if target in ("raw", "raw_server") else SQLITE_ANALYTICS_DB
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn, "sqlite"
