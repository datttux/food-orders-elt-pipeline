"""load_transformed.py

Loads validated and transformed data batches into the MySQL analytics database.
Creates food_orders_analytics_db and cleaned_food_orders table with appropriate types.
Inserts records in batches of 100 using database transactions.
"""

from typing import Iterator
import pandas as pd
import numpy as np
from config import (
    ANALYTICS_DB_NAME,
    ANALYTICS_TABLE_NAME,
    get_db_connection,
    get_mysql_connection
)

ANALYTICS_TABLE_SCHEMA_MYSQL = f"""
CREATE TABLE IF NOT EXISTS {ANALYTICS_TABLE_NAME} (
    order_id INT PRIMARY KEY,
    customer_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    order_date_and_time DATETIME,
    delivery_date_and_time DATETIME,
    order_value DOUBLE NULL,
    delivery_fee DOUBLE NULL,
    payment_method VARCHAR(50),
    discounts_and_offers VARCHAR(100),
    commission_fee DOUBLE NULL,
    payment_processing_fee DOUBLE NULL,
    refunds_chargebacks DOUBLE NULL,
    weather_condition VARCHAR(50),
    order_channel VARCHAR(50),
    delivery_time_minutes DOUBLE NULL,
    order_date DATE,
    order_hour INT,
    platform_net_revenue DOUBLE NULL
) ENGINE=InnoDB;
"""

ANALYTICS_TABLE_SCHEMA_SQLITE = f"""
CREATE TABLE IF NOT EXISTS {ANALYTICS_TABLE_NAME} (
    order_id INTEGER PRIMARY KEY,
    customer_id TEXT,
    restaurant_id TEXT,
    order_date_and_time TEXT,
    delivery_date_and_time TEXT,
    order_value REAL NULL,
    delivery_fee REAL NULL,
    payment_method TEXT,
    discounts_and_offers TEXT,
    commission_fee REAL NULL,
    payment_processing_fee REAL NULL,
    refunds_chargebacks REAL NULL,
    weather_condition TEXT,
    order_channel TEXT,
    delivery_time_minutes REAL NULL,
    order_date TEXT,
    order_hour INTEGER,
    platform_net_revenue REAL NULL
);
"""


def init_analytics_database():
    """Initializes the analytics database and cleaned table."""
    conn, engine = get_db_connection(target="analytics_server")
    cursor = conn.cursor()
    if engine == "mysql":
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {ANALYTICS_DB_NAME};")
        cursor.execute(f"USE {ANALYTICS_DB_NAME};")
        cursor.execute(f"DROP TABLE IF EXISTS {ANALYTICS_TABLE_NAME};")
        cursor.execute(ANALYTICS_TABLE_SCHEMA_MYSQL)
        conn.commit()
    else:
        cursor.execute(f"DROP TABLE IF EXISTS {ANALYTICS_TABLE_NAME};")
        cursor.execute(ANALYTICS_TABLE_SCHEMA_SQLITE)
        conn.commit()
    cursor.close()
    conn.close()
    print(f"[LOAD_ANALYTICS] Initialized {ANALYTICS_DB_NAME}.{ANALYTICS_TABLE_NAME} using {engine} engine.")


class AnalyticsLoader:
    """Handles transactional batch insertion of transformed records into analytics table."""

    def __init__(self):
        init_analytics_database()
        self.conn, self.engine = get_db_connection(target="analytics")
        self.cursor = self.conn.cursor()
        self.insert_sql = f"""
        INSERT INTO {ANALYTICS_TABLE_NAME} (
            order_id, customer_id, restaurant_id, order_date_and_time, delivery_date_and_time,
            order_value, delivery_fee, payment_method, discounts_and_offers, commission_fee,
            payment_processing_fee, refunds_chargebacks, weather_condition, order_channel,
            delivery_time_minutes, order_date, order_hour, platform_net_revenue
        ) VALUES ({', '.join(['?' if self.engine == 'sqlite' else '%s'] * 18)});
        """
        self.total_inserted = 0

    def load_batch(self, batch_df: pd.DataFrame, batch_number: int) -> int:
        """Loads a single batch of validated records using a transaction."""
        if batch_df.empty:
            return 0

        records = []
        for _, row in batch_df.iterrows():
            rec = (
                int(row['order_id']),
                str(row['customer_id']),
                str(row['restaurant_id']),
                str(row['order_date_and_time']),
                str(row['delivery_date_and_time']),
                float(row['order_value']) if pd.notna(row['order_value']) else None,
                float(row['delivery_fee']) if pd.notna(row['delivery_fee']) else None,
                str(row['payment_method']),
                str(row['discounts_and_offers']),
                float(row['commission_fee']) if pd.notna(row['commission_fee']) else None,
                float(row['payment_processing_fee']) if pd.notna(row['payment_processing_fee']) else None,
                float(row['refunds_chargebacks']) if pd.notna(row['refunds_chargebacks']) else None,
                str(row['weather_condition']),
                str(row['order_channel']),
                float(row['delivery_time_minutes']) if pd.notna(row['delivery_time_minutes']) else None,
                str(row['order_date']),
                int(row['order_hour']),
                float(row['platform_net_revenue']) if pd.notna(row['platform_net_revenue']) else None,
            )
            records.append(rec)

        try:
            self.cursor.executemany(self.insert_sql, records)
            self.conn.commit()
            count = len(records)
            self.total_inserted += count
            print(f"[LOAD_ANALYTICS] Batch {batch_number}: inserted {count} records (cumulative: {self.total_inserted})")
            return count
        except Exception as e:
            self.conn.rollback()
            print(f"[LOAD_ANALYTICS] ERROR inserting batch {batch_number}: {e}. Transaction rolled back.")
            raise

    def verify_and_close(self) -> int:
        """Verifies final row count and closes connection."""
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {ANALYTICS_TABLE_NAME};")
            final_count = self.cursor.fetchone()[0]
            print(f"[LOAD_ANALYTICS] Verification SUCCESS: Total records in {ANALYTICS_TABLE_NAME} = {final_count}")
            return final_count
        finally:
            self.cursor.close()
            self.conn.close()
