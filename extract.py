"""extract.py

Extract module for the ELT pipeline.
Reads source CSV data in batches of 100 rows without cleaning or transforming.
"""

from typing import Iterator
import pandas as pd
from config import CSV_FILE_PATH, BATCH_SIZE


def extract_batches(file_path: str = CSV_FILE_PATH, batch_size: int = BATCH_SIZE) -> Iterator[pd.DataFrame]:
    """
    Extracts records from the source CSV file in batches.
    
    Requirements:
    - Read food_orders.csv using Pandas.
    - Extract records in batches of 100 rows.
    - Display the batch number and record count.
    - Do not clean, remove duplicates, or transform data during extraction.
    """
    print(f"\n[EXTRACT] Starting extraction from '{file_path}' with batch size = {batch_size}...")
    
    # Read CSV using chunksize to yield batches without loading entire dataset into memory at once
    chunks = pd.read_csv(file_path, chunksize=batch_size, keep_default_na=True)
    
    batch_num = 1
    total_records = 0
    for chunk in chunks:
        record_count = len(chunk)
        total_records += record_count
        print(f"[EXTRACT] Batch {batch_num}: extracted {record_count} records (cumulative: {total_records})")
        yield chunk
        batch_num += 1

    print(f"[EXTRACT] Extraction completed. Total batches: {batch_num - 1}, Total records: {total_records}")


if __name__ == "__main__":
    count = 0
    for batch in extract_batches():
        count += len(batch)
    print(f"Total extracted records: {count}")

