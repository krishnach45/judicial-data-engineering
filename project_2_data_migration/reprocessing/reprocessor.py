import pandas as pd
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class IterativeReprocessor:

    def load_errors(self, error_file: str) -> pd.DataFrame:
        with open(error_file) as f:
            errors = json.load(f)
        df = pd.DataFrame([e['row'] for e in errors])
        logger.info(f"Loaded {len(df):,} failed records from {error_file}")
        return df

    def apply_fix(self, df: pd.DataFrame, fix_fn) -> pd.DataFrame:
        before   = len(df)
        df_fixed = fix_fn(df)
        logger.info(f"Fix applied: {before:,} → {len(df_fixed):,} rows")
        return df_fixed

    def reprocess(self, error_file, fix_fn, transformer, validator, loader, table):
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info(f"Starting reprocessing — Run: {run_id}")

        df_errors = self.load_errors(error_file)
        df_fixed  = self.apply_fix(df_errors, fix_fn)
        df_clean  = transformer.transform(df_fixed)
        result    = validator.validate_dataframe(df_clean)

        logger.info(
            f"Re-validation: {result['valid_count']:,} passed | "
            f"{result['invalid_count']:,} still failing | "
            f"Pass rate: {result['pass_rate']:.1f}%"
        )

        loaded = loader.load(result['valid_df'], table)
        logger.info(f"Reloaded {loaded:,} rows into {table}")

        if result['errors']:
            new_error_file = error_file.replace('.json', f'_retry_{run_id}.json')
            with open(new_error_file, 'w') as f:
                json.dump(result['errors'], f, indent=2, default=str)
            logger.info(f"{result['invalid_count']:,} records still failing — saved to {new_error_file}")

        return result
