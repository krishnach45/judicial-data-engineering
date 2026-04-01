import pandas as pd
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class CourtDataExtractor:

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        logger.info("Extractor connected to database")

    def extract_table(self, table_name: str):
        query = f"SELECT * FROM {table_name} WHERE _processed = 'false'"
        df = pd.read_sql(query, self.engine)
        logger.info(f"Extracted {len(df):,} rows from {table_name}")
        return df

    def extract_cases(self):
        return self.extract_table("raw_cases")

    def extract_parties(self):
        return self.extract_table("raw_parties")

    def extract_hearings(self):
        return self.extract_table("raw_hearings")

    def extract_charges(self):
        return self.extract_table("raw_charges")

    def extract_judges(self):
        return self.extract_table("raw_judges")

    def get_row_count(self, table_name: str) -> int:
        with self.engine.connect() as conn:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()
        return count
