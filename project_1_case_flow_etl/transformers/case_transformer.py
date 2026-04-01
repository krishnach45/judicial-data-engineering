import pandas as pd
import hashlib
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CaseTransformer:

    VALID_CASE_TYPES = {'criminal', 'civil', 'family', 'traffic', 'probate'}
    VALID_STATUSES   = {'open', 'closed', 'pending', 'dismissed', 'appealed'}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Starting transformation of {len(df):,} rows")
        df = df.copy()
        df = self._drop_system_columns(df)
        df = self._clean_case_numbers(df)
        df = self._standardize_dates(df)
        df = self._standardize_categories(df)
        df = self._hash_pii(df)
        df = self._remove_duplicates(df)
        df = self._add_audit_columns(df)
        logger.info(f"Transformation complete: {len(df):,} rows remaining")
        return df

    def _drop_system_columns(self, df):
        cols_to_drop = ['_loaded_at', '_processed', '_error_msg', '_run_id']
        existing = [c for c in cols_to_drop if c in df.columns]
        return df.drop(columns=existing)

    def _clean_case_numbers(self, df):
        if 'case_number' not in df.columns:
            return df
        def normalize(val):
            if pd.isna(val) or str(val).strip() == '':
                return None
            val = str(val).strip().upper()
            val = re.sub(r'^(CRIM|CR|C)\s*[-/\s]?', 'CR-', val)
            val = re.sub(r'[\s/]+', '-', val)
            return val
        df['case_number'] = df['case_number'].apply(normalize)
        logger.info("Case numbers normalized")
        return df

    def _standardize_dates(self, df):
        date_cols = ['filed_date', 'hearing_date', 'dob', 'appointed_dt']
        for col in date_cols:
            if col in df.columns:
               df[col] = pd.to_datetime(df[col], errors='coerce')
               null_count = df[col].isna().sum()
               if null_count > 0:
                    logger.warning(f"{null_count} unparseable dates in '{col}' set to NULL")
        return df

    def _standardize_categories(self, df):
        cat_cols = ['case_type', 'status', 'party_type', 'charge_type', 'hearing_type', 'disposition']
        for col in cat_cols:
            if col in df.columns:
                df[col] = df[col].str.lower().str.strip()
        if 'case_type' in df.columns:
            invalid = ~df['case_type'].isin(self.VALID_CASE_TYPES) & df['case_type'].notna()
            if invalid.any():
                logger.warning(f"{invalid.sum()} rows have invalid case_type values")
        return df

    def _hash_pii(self, df):
        if 'ssn' in df.columns:
            df['ssn_hash'] = df['ssn'].apply(
                lambda x: hashlib.sha256(str(x).encode()).hexdigest()
                if pd.notna(x) and str(x).strip() != '' else None
            )
            df.drop(columns=['ssn'], inplace=True)
            logger.info("SSN column hashed and removed")
        return df

    def _remove_duplicates(self, df):
        if 'case_number' not in df.columns:
            return df
        before = len(df)
        df = df.drop_duplicates(subset=['case_number'], keep='last')
        removed = before - len(df)
        if removed > 0:
            logger.info(f"Removed {removed:,} duplicate case records")
        return df

    def _add_audit_columns(self, df):
        df['transformed_at'] = datetime.now()
        return df