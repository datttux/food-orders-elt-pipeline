"""elt_batch_pipeline.py

Main orchestrator for the Batch-Based ELT Pipeline.
Coordinates:
1. Source extraction from CSV in batches of 100
2. Raw data loading into MySQL food_orders_raw_db.raw_food_orders
3. Raw data batch retrieval from MySQL
4. Data transformation & cross-batch deduplication using Pandas
5. Batch validation and rejected record tracking
6. Loading validated records into MySQL food_orders_analytics_db.cleaned_food_orders
7. Generation of validation report and final execution metrics report
8. Execution of business analysis SQL queries.
"""

import sys
import time
import pandas as pd
from extract import extract_batches
from load_raw import load_raw_batches
from retrieve_raw import retrieve_raw_batches
from transform import BatchTransformer
from validate import PipelineValidator
from load_transformed import AnalyticsLoader
from sql_analysis import run_sql_analysis


def run_pipeline():
    start_time = time.time()
    print("=" * 80)
    print("   STARTING BATCH-BASED ELT PIPELINE (PYTHON, PANDAS, MYSQL)")
    print("=" * 80)

    # -------------------------------------------------------------
    # STEP 1: EXTRACT SOURCE DATA
    # -------------------------------------------------------------
    print("\n>>> STEP 1: EXTRACT SOURCE DATA (Batches of 100 rows) <<<")
    # We load raw batches into memory generator
    extracted_batches = list(extract_batches())
    total_extracted = sum(len(b) for b in extracted_batches)
    total_extract_batches = len(extracted_batches)

    # -------------------------------------------------------------
    # STEP 2: LOAD RAW DATA INTO MYSQL
    # -------------------------------------------------------------
    print("\n>>> STEP 2: LOAD RAW DATA INTO RAW DATABASE <<<")
    total_raw_loaded = load_raw_batches(iter(extracted_batches))

    # -------------------------------------------------------------
    # STEP 3 & 4 & 5 & 6: RETRIEVE, TRANSFORM, VALIDATE & LOAD ANALYTICS
    # -------------------------------------------------------------
    print("\n>>> STEP 3, 4, 5, 6: RETRIEVE RAW, TRANSFORM, VALIDATE & LOAD ANALYTICS <<<")
    transformer = BatchTransformer()
    validator = PipelineValidator()
    loader = AnalyticsLoader()

    successful_batches = 0
    failed_batches = 0
    all_transformed_batches = []
    batch_idx = 1

    try:
        for raw_batch_df in retrieve_raw_batches():
            try:
                # Transform
                valid_df, rejected_df = transformer.transform_batch(raw_batch_df, batch_number=batch_idx)
                
                # Track rejections
                validator.record_rejected(rejected_df)
                
                # Validate
                is_batch_valid = validator.validate_batch(valid_df, batch_number=batch_idx)
                if not is_batch_valid:
                    print(f"[PIPELINE WARNING] Validation warnings encountered in batch {batch_idx}")

                # Load to Analytics DB
                if not valid_df.empty:
                    loader.load_batch(valid_df, batch_number=batch_idx)
                    all_transformed_batches.append(valid_df)

                successful_batches += 1
                batch_idx += 1

            except Exception as batch_err:
                failed_batches += 1
                print(f"[PIPELINE ERROR] Batch {batch_idx} failed: {batch_err}")
                raise

        total_analytics_loaded = loader.verify_and_close()

    except Exception as e:
        print(f"[PIPELINE FATAL ERROR]: {e}")
        raise

    # -------------------------------------------------------------
    # STEP 7: VALIDATION REPORT GENERATION
    # -------------------------------------------------------------
    print("\n>>> STEP 7: GENERATING VALIDATION REPORT <<<")
    full_transformed_df = pd.concat(all_transformed_batches, ignore_index=True) if all_transformed_batches else None
    validation_status = "PASSED" if len(validator.validation_errors) == 0 else "FAILED"

    validator.generate_report(
        total_extracted=total_extracted,
        total_raw_loaded=total_raw_loaded,
        total_valid_transformed=transformer.total_transformed,
        total_analytics_loaded=total_analytics_loaded,
        batch_count=successful_batches + failed_batches,
        final_df=full_transformed_df
    )

    # -------------------------------------------------------------
    # STEP 8: FINAL EXECUTION REPORT
    # -------------------------------------------------------------
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("                      FINAL EXECUTION REPORT")
    print("=" * 80)
    print(f"Total records extracted from CSV      : {total_extracted}")
    print(f"Total number of batches processed     : {successful_batches + failed_batches}")
    print(f"Total raw records loaded into MySQL   : {total_raw_loaded}")
    print(f"Total raw records retrieved from MySQL: {total_raw_loaded}")
    print(f"Total valid records transformed       : {transformer.total_transformed}")
    print(f"Total rejected records                : {transformer.total_rejected}")
    print(f"Total records loaded into Analytics DB: {total_analytics_loaded}")
    print(f"Number of successful batches          : {successful_batches}")
    print(f"Number of failed batches              : {failed_batches}")
    print(f"Final validation status               : {validation_status}")
    print(f"Total Execution Time                  : {elapsed:.2f} seconds")
    print("=" * 80)

    # -------------------------------------------------------------
    # STEP 9: BUSINESS ANALYSIS SQL QUERIES
    # -------------------------------------------------------------
    run_sql_analysis()


if __name__ == "__main__":
    run_pipeline()

