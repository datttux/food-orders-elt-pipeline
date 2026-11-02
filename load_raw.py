"""load_raw.py

Loads raw extracted batches into the MySQL raw database.
Preserves original values, missing values, and duplicate records.
Uses database transactions for batch insertion.
"""

from typing import Iterator
import pandas as pd
import numpy as np
from config import (
    RAW_DB_NAME,
    RAW_TABLE_NAME,
    get_db_connection,
    get_mysql_connection
)

RAW_TABLE_SCHEMA_MYSQL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
    order_id INT,
    customer_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    order_date_and_time VARCHAR(50),
    delivery_date_and_time VARCHAR(50),
    order_value DOUBLE NULL,
    delivery_fee DOUBLE NULL,
    payment_method VARCHAR(50) NULL,
    discounts_and_offers VARCHAR(100) NULL,
    commission_fee DOUBLE NULL,
    payment_processing_fee DOUBLE NULL,
    refunds_chargebacks DOUBLE NULL,
    weather_condition VARCHAR(50) NULL,
    order_channel VARCHAR(50) NULL
) ENGINE=InnoDB;
"""

RAW_TABLE_SCHEMA_SQLITE = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
    order_id INTEGER,
    customer_id TEXT,
    restaurant_id TEXT,
    order_date_and_time TEXT,
    delivery_date_and_time TEXT,
    order_value REAL NULL,
    delivery_fee REAL NULL,
    payment_method TEXT NULL,
    discounts_and_offers TEXT NULL,
    commission_fee REAL NULL,
    payment_processing_fee REAL NULL,
    refunds_chargebacks REAL NULL,
    weather_condition TEXT NULL,
    order_channel TEXT NULL
);
"""


def init_raw_database():
    """Initializes the raw database and raw table."""
    conn, engine = get_db_connection(target="raw_server")
    cursor = conn.cursor()
    if engine == "mysql":
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {RAW_DB_NAME};")
        cursor.execute(f"USE {RAW_DB_NAME};")
        cursor.execute(f"DROP TABLE IF EXISTS {RAW_TABLE_NAME};")
        cursor.execute(RAW_TABLE_SCHEMA_MYSQL)
        conn.commit()
    else:
        # SQLite
        cursor.execute(f"DROP TABLE IF EXISTS {RAW_TABLE_NAME};")
        cursor.execute(RAW_TABLE_SCHEMA_SQLITE)
        conn.commit()
    cursor.close()
    conn.close()
    print(f"[LOAD_RAW] Initialized {RAW_DB_NAME}.{RAW_TABLE_NAME} using {engine} engine.")


def load_raw_batches(batches: Iterator[pd.DataFrame]) -> int:
    """
    Loads raw DataFrame batches into raw database using transactions.
    Returns total records loaded.
    """
    init_raw_database()
    conn, engine = get_db_connection(target="raw")
    cursor = conn.cursor()
    
    insert_sql = f"""
    INSERT INTO {RAW_TABLE_NAME} (
        order_id, customer_id, restaurant_id, order_date_and_time, delivery_date_and_time,
        order_value, delivery_fee, payment_method, discounts_and_offers, commission_fee,
        payment_processing_fee, refunds_chargebacks, weather_condition, order_channel
    ) VALUES ({', '.join(['?' if engine == 'sqlite' else '%s'] * 14)});
    """
    
    total_loaded = 0
    batch_idx = 1
    
    try:
        for batch_df in batches:
            records = []
            for _, row in batch_df.iterrows():
                # Convert NaN to None for proper SQL NULL preservation
                rec = (
                    int(row['Order ID']) if pd.notna(row['Order ID']) else None,
                    str(row['Customer ID']) if pd.notna(row['Customer ID']) else None,
                    str(row['Restaurant ID']) if pd.notna(row['Restaurant ID']) else None,
                    str(row['Order Date and Time']) if pd.notna(row['Order Date and Time']) else None,
                    str(row['Delivery Date and Time']) if pd.notna(row['Delivery Date and Time']) else None,
                    float(row['Order Value']) if pd.notna(row['Order Value']) else None,
                    float(row['Delivery Fee']) if pd.notna(row['Delivery Fee']) else None,
                    str(row['Payment Method']) if pd.notna(row['Payment Method']) else None,
                    str(row['Discounts and Offers']) if pd.notna(row['Discounts and Offers']) else None,
                    float(row['Commission Fee']) if pd.notna(row['Commission Fee']) else None,
                    float(row['Payment Processing Fee']) if pd.notna(row['Payment Processing Fee']) else None,
                    float(row['Refunds/Chargebacks']) if pd.notna(row['Refunds/Chargebacks']) else None,
                    str(row['Weather Condition']) if pd.notna(row['Weather Condition']) else None,
                    str(row['Order Channel']) if pd.notna(row['Order Channel']) else None,
                )
                records.append(rec)
            
            # Execute batch insert using database transaction
            cursor.executemany(insert_sql, records)
            conn.commit()  # commit transaction for each batch
            
            batch_count = len(records)
            total_loaded += batch_count
            print(f"[LOAD_RAW] Batch {batch_idx}: inserted {batch_count} records (cumulative: {total_loaded})")
            batch_idx += 1
            
        # Verification
        cursor.execute(f"SELECT COUNT(*) FROM {RAW_TABLE_NAME};")
        row_count = cursor.fetchone()[0]
        print(f"[LOAD_RAW] Verification SUCCESS: Total records in {RAW_TABLE_NAME} = {row_count}")
        return row_count
        
    except Exception as e:
        conn.rollback()
        print(f"[LOAD_RAW] ERROR during batch insertion: {e}. Transaction rolled back.")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    from extract import extract_batches
    load_raw_batches(extract_batches())
