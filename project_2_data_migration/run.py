import logging
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer
from project_1_case_flow_etl.validators.case_validator import CaseValidator
from project_1_case_flow_etl.loaders.case_loader import CaseLoader
from project_2_data_migration.sampling.sampler import MigrationSampler
from project_2_data_migration.conversion.migration_engine import MigrationEngine
from project_2_data_migration.reprocessing.reprocessor import IterativeReprocessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://court_admin:court_pass@127.0.0.1:5433/court_dw'
)

def run():
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    engine = create_engine(DATABASE_URL)

    # ── Phase 1: Sample & Profile ───────────────────────────────────────────
    logger.info("Phase 1 — Sampling and profiling source data")
    sampler = MigrationSampler(DATABASE_URL)
    sample  = sampler.stratified_sample('raw_cases', 'case_type', 0.10)
    profile = sampler.profile(sample)
    logger.info(f"Sample pass rate preview: {profile['row_count']:,} rows profiled")

    # ── Phase 2: Full Migration ─────────────────────────────────────────────
    logger.info("Phase 2 — Running full migration")
    migration = MigrationEngine(engine, engine, run_id)
    transformer = CaseTransformer()
    validator   = CaseValidator()
    loader      = CaseLoader(DATABASE_URL)

    result = migration.run('raw_cases', transformer, validator, loader)

    # ── Phase 3: Reprocess Failures ─────────────────────────────────────────
    import glob
    error_files = glob.glob(f'migration_errors/raw_cases_{run_id}.json')

    if error_files:
        logger.info("Phase 3 — Reprocessing failed records")
        reprocessor = IterativeReprocessor()

        def fix_null_case_types(df):
            """Fix: fill null case_type with 'civil' as default"""
            df['case_type'] = df['case_type'].fillna('civil')
            return df

        reprocessor.reprocess(
            error_file=error_files[0],
            fix_fn=fix_null_case_types,
            transformer=transformer,
            validator=validator,
            loader=loader,
            table='clean_cases'
        )
    else:
        logger.info("Phase 3 — No failed records to reprocess")

    logger.info("Migration pipeline complete")
    logger.info(f"  Valid:     {result['valid_count']:,}")
    logger.info(f"  Invalid:   {result['invalid_count']:,}")
    logger.info(f"  Pass rate: {result['pass_rate']:.1f}%")

if __name__ == '__main__':
    run()