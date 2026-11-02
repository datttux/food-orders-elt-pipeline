"""retrieve_raw.py

Retrieves raw records from the MySQL raw table in batches of 100 rows.
Loads each retrieved batch into a Pandas DataFrame.
Preserves the raw database table untouched.
"""

from typing import Iterator
import pandas as pd
from config import RAW_TABLE_NAME, BATCH_SIZE, get_db_connection


COLUMNS = [
    'order_id', 'customer_id', 'restaurant_id', 'order_date_and_time', 'delivery_date_and_time',
    'order_value', 'delivery_fee', 'payment_method', 'discounts_and_offers', 'commission_fee',
    'payment_processing_fee', 'refunds_chargebacks', 'weather_condition', 'order_channel'
]


def retrieve_raw_batches(batch_size: int = BATCH_SIZE) -> Iterator[pd.DataFrame]:
    """
    Connects to raw database and retrieves records in batches of 100 rows.
    Loads each batch into a Pandas DataFrame.
    """
    conn, engine = get_db_connection(target="raw")
    cursor = conn.cursor()
    
    try:
        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM {RAW_TABLE_NAME};")
        total_records = cursor.fetchone()[0]
        print(f"\n[RETRIEVE_RAW] Total records in {RAW_TABLE_NAME}: {total_records}. Retrieving in batches of {batch_size}...")
        
        offset = 0
        batch_num = 1
        
        while offset < total_records:
            query = f"SELECT * FROM {RAW_TABLE_NAME} LIMIT {batch_size} OFFSET {offset};"
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                break
                
            batch_data = [list(r) for r in rows]
            batch_df = pd.DataFrame(batch_data, columns=COLUMNS)
            
            record_count = len(batch_df)
            print(f"[RETRIEVE_RAW] Batch {batch_num}: retrieved {record_count} records (offset: {offset})")
            
            yield batch_df
            
            offset += record_count
            batch_num += 1
            
        print(f"[RETRIEVE_RAW] Retrieval complete. Retrieved {offset} records across {batch_num - 1} batches.")
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    count = 0
    for b in retrieve_raw_batches():
        count += len(b)
    print(f"Total retrieved: {count}")

