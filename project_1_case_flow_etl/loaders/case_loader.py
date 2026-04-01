import pandas as pd
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class CaseLoader:

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def load(self, df: pd.DataFrame, table: str):
        if df.empty:
            logger.warning(f"No rows to load into {table}")
            return 0

        staging = f"{table}_staging"
        df.to_sql(staging, self.engine,
                  if_exists='replace', index=False,
                  chunksize=1000, method='multi')
        logger.info(f"Staged {len(df):,} rows into {staging}")

        with self.engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {table}
                SELECT * FROM {staging}
                ON CONFLICT (case_number)
                DO UPDATE SET
                    status         = EXCLUDED.status,
                    case_type      = EXCLUDED.case_type,
                    transformed_at = EXCLUDED.transformed_at
            """))
            conn.commit()

        logger.info(f"Upserted {len(df):,} rows into {table}")
        return len(df)
