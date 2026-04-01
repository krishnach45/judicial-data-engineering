import logging
import os
from project_1_case_flow_etl.extractors.court_extractor import CourtDataExtractor
from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer
from project_1_case_flow_etl.validators.case_validator import CaseValidator
from project_1_case_flow_etl.loaders.case_loader import CaseLoader
import json
from pathlib import Path
from datetime import datetime

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
    run_id  = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger.info(f"Starting ETL pipeline — Run ID: {run_id}")

    # Initialize components
    extractor  = CourtDataExtractor(DATABASE_URL)
    transformer = CaseTransformer()
    validator  = CaseValidator()
    loader     = CaseLoader(DATABASE_URL)

    # Extract
    df_raw = extractor.extract_cases()

    # Transform
    df_clean = transformer.transform(df_raw)

    # Validate
    result = validator.validate_dataframe(df_clean)

    # Save errors for reprocessing
    if result['errors']:
        error_dir = Path('migration_errors')
        error_dir.mkdir(exist_ok=True)
        error_file = error_dir / f'cases_{run_id}.json'
        with open(error_file, 'w') as f:
            json.dump(result['errors'], f, indent=2, default=str)
        logger.info(f"Saved {result['invalid_count']:,} errors to {error_file}")

    # Load valid rows only
    loaded = loader.load(result['valid_df'], 'clean_cases')

    logger.info(f"Pipeline complete — Run ID: {run_id}")
    logger.info(f"  Extracted:  {len(df_raw):,}")
    logger.info(f"  Valid:      {result['valid_count']:,}")
    logger.info(f"  Invalid:    {result['invalid_count']:,}")
    logger.info(f"  Loaded:     {loaded:,}")
    logger.info(f"  Pass rate:  {result['pass_rate']:.1f}%")

if __name__ == '__main__':
    run()