import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

class MigrationSampler:

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def stratified_sample(self, table: str, strata_col: str,
                          sample_pct: float = 0.10) -> pd.DataFrame:
        df = pd.read_sql(f"SELECT * FROM {table}", self.engine)

        # Fill nulls so groupby does not drop them
        df[strata_col] = df[strata_col].fillna('unknown')

        # Sample each group independently and concat
        groups = []
        for val, group in df.groupby(strata_col):
            n = max(1, int(len(group) * sample_pct))
            groups.append(group.sample(n=n, random_state=42))

        sample = pd.concat(groups, ignore_index=True)

        logger.info(f"Sample: {len(sample):,} rows ({sample_pct*100:.0f}% of {len(df):,})")
        logger.info(f"Strata distribution:\n{sample[strata_col].value_counts()}")
        return sample

    def profile(self, df: pd.DataFrame) -> dict:
        profile = {
            'row_count':     len(df),
            'column_count':  len(df.columns),
            'null_counts':   df.isnull().sum().to_dict(),
            'null_pct':      (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
            'unique_counts': df.nunique().to_dict(),
            'dtypes':        df.dtypes.astype(str).to_dict(),
        }
        high_null = {col: pct for col, pct in profile['null_pct'].items() if pct > 20}
        if high_null:
            logger.warning(f"High null columns (>20%): {high_null}")
        return profile

    def full_extract(self, table: str) -> pd.DataFrame:
        df = pd.read_sql(f"SELECT * FROM {table}", self.engine)
        logger.info(f"Full extract: {len(df):,} rows from {table}")
        return df