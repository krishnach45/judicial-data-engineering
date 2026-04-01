import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
logger = logging.getLogger(__name__)


# ── CONNECTION ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://court_admin:court_pass@127.0.0.1:5433/court_dw'
)

engine = create_engine(DATABASE_URL)


# ── RAW TABLE DEFINITIONS ──────────────────────────────────────────────────
# Maps table name → CSV file path
TABLES = {
    'raw_cases':    'data/synthetic/cases.csv',
    'raw_parties':  'data/synthetic/parties.csv',
    'raw_hearings': 'data/synthetic/hearings.csv',
    'raw_charges':  'data/synthetic/charges.csv',
    'raw_judges':   'data/synthetic/judges.csv',
}


# ── VERIFY FILES EXIST ─────────────────────────────────────────────────────
def verify_files():
    logger.info('Verifying source CSV files...')
    missing = []
    for table, filepath in TABLES.items():
        if not Path(filepath).exists():
            missing.append(filepath)
            logger.error(f'Missing file: {filepath}')
        else:
            logger.info(f'Found: {filepath}')
    if missing:
        raise FileNotFoundError(
            f'Run generate_data.py first. Missing files: {missing}'
        )


# ── DROP RAW TABLES ────────────────────────────────────────────────────────
def drop_raw_tables():
    """
    Drop all raw tables before reloading.
    Ensures a clean slate for every seed run — supports repeatability.
    """
    logger.info('Dropping existing raw tables...')
    with engine.connect() as conn:
        for table in reversed(list(TABLES.keys())):
            conn.execute(text(f'DROP TABLE IF EXISTS {table} CASCADE'))
            logger.info(f'Dropped: {table}')
        conn.commit()


# ── LOAD TABLE ─────────────────────────────────────────────────────────────
def load_table(table_name: str, filepath: str):
    """
    Load a single CSV into PostgreSQL as a raw_ table.
    All columns loaded as TEXT — type casting happens in the pipeline,
    not here. This preserves the dirty data exactly as it came from source.
    """
    logger.info(f'Loading {filepath} → {table_name}...')

    # Read all columns as string — never coerce types at seed stage
    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)

    # Replace empty strings with None (NULL in DB)
    df = df.replace('', None)

    row_count = len(df)
    logger.info(f'  Read {row_count:,} rows from {filepath}')

    # Load into PostgreSQL
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',   # Drop and recreate — idempotent
        index=False,
        chunksize=1000,        # Batch inserts for performance
        method='multi'
    )

    logger.info(f'  Loaded {row_count:,} rows into {table_name}')
    return row_count


# ── VERIFY ROW COUNTS ──────────────────────────────────────────────────────
def verify_row_counts():
    """
    After loading, compare DB row counts against CSV row counts.
    Flags any discrepancy before the pipeline runs.
    """
    logger.info('Verifying row counts...')
    all_match = True

    with engine.connect() as conn:
        for table, filepath in TABLES.items():
            csv_count = len(pd.read_csv(filepath, dtype=str))
            db_count  = conn.execute(
                text(f'SELECT COUNT(*) FROM {table}')
            ).scalar()

            status = '✅' if csv_count == db_count else '❌'
            logger.info(
                f'  {status} {table}: CSV={csv_count:,} | DB={db_count:,}'
            )
            if csv_count != db_count:
                all_match = False

    if not all_match:
        raise ValueError('Row count mismatch detected — check logs above')
    logger.info('All row counts verified successfully')


# ── ADD METADATA COLUMNS ───────────────────────────────────────────────────
def add_metadata_columns():
    """
    Add tracking columns to every raw table.
    These are used by the pipeline to track processing status.
    """
    logger.info('Adding metadata columns...')
    with engine.connect() as conn:
        for table in TABLES.keys():
            conn.execute(text(f'''
                ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS _loaded_at    TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS _processed    BOOLEAN   DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS _error_msg    TEXT      DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS _run_id       TEXT      DEFAULT NULL
            '''))
            logger.info(f'  Added metadata columns to {table}')
        conn.commit()


# ── PRINT SUMMARY ──────────────────────────────────────────────────────────
def print_summary():
    logger.info('─' * 50)
    logger.info('SEED SUMMARY')
    logger.info('─' * 50)
    with engine.connect() as conn:
        for table in TABLES.keys():
            count = conn.execute(
                text(f'SELECT COUNT(*) FROM {table}')
            ).scalar()
            logger.info(f'  {table:<20} {count:>10,} rows')
    logger.info('─' * 50)
    logger.info('Raw tables ready. Run the ETL pipeline next:')
    logger.info('  python project_1_case_flow_etl/run.py')


# ── MAIN ───────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    logger.info('Starting raw table seed...')

    try:
        verify_files()
        drop_raw_tables()

        total_rows = 0
        for table_name, filepath in TABLES.items():
            rows = load_table(table_name, filepath)
            total_rows += rows

        add_metadata_columns()
        verify_row_counts()
        print_summary()

        logger.info(f'Seed complete. Total rows loaded: {total_rows:,}')

    except FileNotFoundError as e:
        logger.error(f'File error: {e}')
        raise
    except Exception as e:
        logger.error(f'Seed failed: {e}')
        raise