# Online Food Delivery Data Engineering & Analytics Project
### Batch-Based ELT-Style Pipeline & Exploratory Data Analysis

This repository contains the complete solution for the Food Orders data analysis and engineering assignments:
1. **Interactive Data Analysis Notebook (`food_orders_analysis.ipynb`)**: Comprehensively covers Tasks 1 through 8 (Data Understanding, Data Quality Assessment, Cleaning Rules Formulation, Cleaning Execution, Feature Derivation, Business Analytics, Visualizations, and Validation).
2. **Production Batch ELT Pipeline (`elt_batch_pipeline.py`)**: A modular, batch-based pipeline (100 rows per batch) extracting raw data from CSV, staging it into a raw database, retrieving it in batches, cleaning/transforming it with Pandas, validating business rules, and loading cleaned data into an analytics database for SQL reporting.

---

## 1. Project Directory Structure

```text
food/
├── food_orders.csv                                                  # Source dataset (1,010 records)
├── food_orders_analysis.ipynb                                       # Interactive Jupyter Notebook (Tasks 1–8)
├── config.py                                                        # Central configuration, file paths, and DB settings
├── extract.py                                                       # Ingests source CSV in chunks of 100 rows
├── load_raw.py                                                      # Inserts raw batches into raw database
├── retrieve_raw.py                                                  # Streams batches of 100 from raw database
├── transform.py                                                     # Cleans, deduplicates (cross-batch), derives metrics
├── validate.py                                                      # Audits data quality, logs rejected records & report
├── load_transformed.py                                              # Loads validated batches into analytics database
├── elt_batch_pipeline.py                                            # Master orchestrator running the full workflow
├── sql_queries.sql                                                  # Pure SQL queries for business analysis
├── sql_analysis.py                                                  # Python executor for the SQL business queries
├── validation_report.txt                                            # Generated audit and validation report
├── rejected_records.csv                                             # Quarantined invalid/duplicate records
├── PROJECT_NOTES.txt                                                # Plain-text guide explaining the whole project
├── requirements.txt                                                 # Python library dependencies
└── README.md                                                        # Project documentation
```

---

## 2. Database Architecture & Setup

The ELT pipeline implements a two-stage database pattern:
* **Raw Database (`food_orders_raw_db`)**:
  * **Table:** `raw_food_orders`
  * **Purpose:** Preserves source fidelity, storing exact original values, missing values (`NULL`), and duplicates without premature alteration.
* **Analytics Database (`food_orders_analytics_db`)**:
  * **Table:** `cleaned_food_orders`
  * **Purpose:** Stores cleaned, deduplicated, validated records with typed schemas and derived analytical features (`delivery_time_minutes`, `order_date`, `order_hour`, `platform_net_revenue`).

### Database Configuration (`config.py`)
Database parameters can be configured directly in `config.py` or overridden via environment variables:
* `DB_ENGINE`: Set to `'mysql'` (default) or `'sqlite'` (for local/offline execution verification).
* `MYSQL_HOST`: Defaults to `127.0.0.1` (or `localhost`).
* `MYSQL_PORT`: Defaults to `3306`.
* `MYSQL_USER`: MySQL username (defaults to `root`).
* `MYSQL_PASSWORD`: MySQL password (defaults to `""`).

> **Note on Seamless Fallback:** If MySQL credentials are not supplied or MySQL is unreachable, the pipeline automatically falls back to SQLite (`food_orders_raw.db` and `food_orders_analytics.db`) to enable full end-to-end execution without disruption.

---

## 3. Data Cleaning & Transformation Rules

| Data Quality Issue | Chosen Handling Rule | Business Assumption / Rationale |
|:---|:---|:---|
| **Duplicate records** | Reject subsequent occurrences across batches using a global seen `order_id` tracker. | Preserves true order volume; prevents double-counting gross revenue and commission. |
| **Missing Order Value** | Retain as `NULL`; exclude from average/total ticket size calculations. | Missing values cannot be assumed to be \$0 or mean without skewing business metrics. |
| **Missing Fee / Refund Attributes** | Retain as `NULL`. | Imputing \$0 refund risks understating customer dissatisfaction and chargeback losses. |
| **Missing Payment Method** | Impute with `'Unknown'`. | Preserves transaction validity while clearly tagging unrecorded payment types. |
| **Missing Discounts & Offers** | Impute with `'Not Specified'`. | Missing promotional detail does not imply zero discount; avoids false campaign claims. |
| **Invalid Dates / Negative Durations** | Reject and log to `rejected_records.csv`. | Deliveries cannot happen before orders; chronologically corrupt records are quarantined. |

### Derived Columns
1. `delivery_time_minutes`: Calculated as `(delivery_date_and_time - order_date_and_time)` in minutes.
2. `order_date`: Date component (`YYYY-MM-DD`).
3. `order_hour`: Hour of placement (`0`–`23`).
4. `platform_net_revenue`:
   $$\text{Commission Fee} + \text{Delivery Fee} - \text{Payment Processing Fee} - \text{Refunds/Chargebacks}$$
   *Strictly computed only when all 4 financial components exist; otherwise left as `NULL`.*

---

## 4. Execution Commands

### Prerequisites
Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Complete ELT Pipeline
To run the automated batch pipeline end-to-end:
```bash
python3 elt_batch_pipeline.py
```
This will:
1. Extract 1,010 records from `food_orders.csv` in batches of 100.
2. Load all 1,010 raw records into `food_orders_raw_db.raw_food_orders`.
3. Stream records back in batches of 100.
4. Transform, deduplicate, and calculate metrics.
5. Validate batch integrity and quarantine rejected records into `rejected_records.csv`.
6. Load the 1,000 validated records into `food_orders_analytics_db.cleaned_food_orders`.
7. Generate `validation_report.txt`.
8. Print the Final Execution Report and run the Business Analysis SQL queries.

### Running SQL Analysis Independently
```bash
python3 sql_analysis.py
```

### Running the Interactive Notebook
```bash
jupyter notebook food_orders_analysis.ipynb
```

---

## 5. Sample Execution Output

### Final Execution Report
```text
================================================================================
                      FINAL EXECUTION REPORT
================================================================================
Total records extracted from CSV      : 1010
Total number of batches processed     : 11
Total raw records loaded into MySQL   : 1010
Total raw records retrieved from MySQL: 1010
Total valid records transformed       : 1000
Total rejected records                : 10
Total records loaded into Analytics DB: 1000
Number of successful batches          : 11
Number of failed batches              : 0
Final validation status               : PASSED
Total Execution Time                  : 0.22 seconds
================================================================================
```

### Business Analysis SQL Results
```text
================================================================================
           BUSINESS ANALYSIS USING SQL (cleaned_food_orders)
================================================================================
1. Total Order Value:                 $1,013,415.00
2. Average Order Value:               $1,055.64
3. Top Payment Method (known):        Cash on Delivery (336 orders)
4. Top Order Channel:                 Mobile App (686 orders)
5. Average Delivery Time:             73.58 minutes
6. Total Platform Net Revenue:        $80,877.00 (from 828 orders)
================================================================================
```

