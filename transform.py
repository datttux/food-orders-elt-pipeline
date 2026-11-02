"""transform.py

Transform module for the ELT pipeline.
Cleans and transforms DataFrames retrieved from the raw database.
Tracks cross-batch duplicates, parses datetimes, derives operational metrics,
handles categorical and financial missing values according to business rules,
and isolates invalid or rejected records.
"""

from typing import Tuple, Set, Optional
import numpy as np
import pandas as pd


class BatchTransformer:
    """
    Stateful batch transformer to ensure duplicate handling across different batches.
    """

    def __init__(self):
        # Global set to track seen order IDs across all batches
        self.seen_order_ids: Set[int] = set()
        self.total_transformed = 0
        self.total_rejected = 0

    def transform_batch(self, batch_df: pd.DataFrame, batch_number: int = 1) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Transforms a single batch of raw food order records.
        Returns:
            (valid_df, rejected_df)
        """
        df = batch_df.copy()
        rejected_records = []
        valid_indices = []

        # 1. Cross-batch and intra-batch duplicate check
        for idx, row in df.iterrows():
            order_id = row['order_id']
            if pd.isna(order_id):
                rejected_records.append({**row.to_dict(), 'rejection_reason': 'Missing Order ID', 'batch_num': batch_number})
                continue
            
            order_id = int(order_id)
            if order_id in self.seen_order_ids:
                rejected_records.append({**row.to_dict(), 'rejection_reason': f'Duplicate Order ID: {order_id}', 'batch_num': batch_number})
                continue
            
            # Register seen order id
            self.seen_order_ids.add(order_id)
            valid_indices.append(idx)

        def finish(res_work, recs):
            rej_df = pd.DataFrame(recs) if recs else pd.DataFrame()
            self.total_transformed += len(res_work)
            self.total_rejected += len(rej_df)
            return res_work, rej_df

        # Subset to non-duplicate rows
        work_df = df.loc[valid_indices].copy()

        if work_df.empty:
            return finish(pd.DataFrame(), rejected_records)

        # 2. Datetime Parsing & Invalid Timestamp Identification
        order_dt = pd.to_datetime(work_df['order_date_and_time'], errors='coerce')
        deliv_dt = pd.to_datetime(work_df['delivery_date_and_time'], errors='coerce')

        bad_time_mask = order_dt.isna() | deliv_dt.isna()
        if bad_time_mask.any():
            for idx, row in work_df[bad_time_mask].iterrows():
                rejected_records.append({**row.to_dict(), 'rejection_reason': 'Invalid or Unparseable Timestamps', 'batch_num': batch_number})
            work_df = work_df[~bad_time_mask].copy()
            order_dt = order_dt[~bad_time_mask]
            deliv_dt = deliv_dt[~bad_time_mask]

        if work_df.empty:
            return finish(pd.DataFrame(), rejected_records)

        # 3. Delivery Duration Calculation & Negative Check
        duration_minutes = (deliv_dt - order_dt).dt.total_seconds() / 60.0
        neg_duration_mask = duration_minutes < 0
        if neg_duration_mask.any():
            for idx, row in work_df[neg_duration_mask].iterrows():
                rejected_records.append({**row.to_dict(), 'rejection_reason': 'Negative Delivery Duration (Delivery before Order)', 'batch_num': batch_number})
            work_df = work_df[~neg_duration_mask].copy()
            order_dt = order_dt[~neg_duration_mask]
            deliv_dt = deliv_dt[~neg_duration_mask]
            duration_minutes = duration_minutes[~neg_duration_mask]

        if work_df.empty:
            return finish(pd.DataFrame(), rejected_records)

        # Assign converted timestamps and derived delivery time
        work_df['order_date_and_time'] = order_dt
        work_df['delivery_date_and_time'] = deliv_dt
        work_df['delivery_time_minutes'] = duration_minutes.round(2)
        work_df['order_date'] = order_dt.dt.strftime('%Y-%m-%d')
        work_df['order_hour'] = order_dt.dt.hour

        # 4. Financial column conversion & preservation of missing values
        financial_cols = ['order_value', 'delivery_fee', 'commission_fee', 'payment_processing_fee', 'refunds_chargebacks']
        for col in financial_cols:
            work_df[col] = pd.to_numeric(work_df[col], errors='coerce')

        # 5. Categorical Missing Value Handling (Business Rules)
        # Payment Method: label missing as 'Unknown'
        work_df['payment_method'] = work_df['payment_method'].fillna('Unknown')
        # Discounts and Offers: label missing as 'Not Specified'
        work_df['discounts_and_offers'] = work_df['discounts_and_offers'].fillna('Not Specified')

        # 6. Platform Net Revenue Calculation
        # Formula: Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
        # Strict rule: Calculate ONLY when all 4 components are available. Otherwise remain NaN/None.
        rev_components = ['commission_fee', 'delivery_fee', 'payment_processing_fee', 'refunds_chargebacks']
        rev_mask = work_df[rev_components].notna().all(axis=1)

        work_df['platform_net_revenue'] = np.nan
        work_df.loc[rev_mask, 'platform_net_revenue'] = (
            work_df.loc[rev_mask, 'commission_fee'] +
            work_df.loc[rev_mask, 'delivery_fee'] -
            work_df.loc[rev_mask, 'payment_processing_fee'] -
            work_df.loc[rev_mask, 'refunds_chargebacks']
        ).round(2)

        return finish(work_df, rejected_records)
