import great_expectations as gx
import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def run_checkpoint(connection_string: str, suite_name: str = "court_cases_suite"):
    """
    Run validation checkpoint against clean_cases table.
    Generates HTML report in great_expectations/uncommitted/data_docs/
    """
    context = gx.get_context()

    # Load data from clean_cases
    engine = create_engine(connection_string)
    df = pd.read_sql("SELECT * FROM clean_cases", engine)
    logger.info(f"Loaded {len(df):,} rows from clean_cases for validation")

    # Create batch
    datasource = context.sources.add_or_update_pandas(name="court_data")
    asset      = datasource.add_dataframe_asset(name="clean_cases")
    batch_req  = asset.build_batch_request(dataframe=df)

    # Run validation
    results = context.run_checkpoint(
        checkpoint_name="court_cases_checkpoint",
        validations=[{
            "batch_request":        batch_req,
            "expectation_suite_name": suite_name,
        }]
    )

    # Build docs
    context.build_data_docs()

    success = results.success
    logger.info(f"Checkpoint result: {'✅ PASSED' if success else '❌ FAILED'}")
    return results
