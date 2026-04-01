import sys
import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://court_admin:court_pass@127.0.0.1:5433/court_dw'
)

default_args = {
    'owner':            'data_engineering',
    'retries':          3,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
    'sla':              timedelta(hours=2),
}


# ── TASK FUNCTIONS ──────────────────────────────────────────────────────────

def task_extract(**context):
    from project_1_case_flow_etl.extractors.court_extractor import CourtDataExtractor
    extractor = CourtDataExtractor(DATABASE_URL)
    count     = extractor.get_row_count('raw_cases')
    logger.info(f"Extract complete — {count:,} rows available")
    context['ti'].xcom_push(key='raw_row_count', value=count)


def task_transform(**context):
    from project_1_case_flow_etl.extractors.court_extractor import CourtDataExtractor
    from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer
    extractor   = CourtDataExtractor(DATABASE_URL)
    transformer = CaseTransformer()
    df_raw      = extractor.extract_cases()
    df_clean    = transformer.transform(df_raw)
    logger.info(f"Transform complete — {len(df_clean):,} rows")
    context['ti'].xcom_push(key='transformed_count', value=len(df_clean))


def task_validate(**context):
    from project_1_case_flow_etl.extractors.court_extractor import CourtDataExtractor
    from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer
    from project_1_case_flow_etl.validators.case_validator import CaseValidator
    extractor   = CourtDataExtractor(DATABASE_URL)
    transformer = CaseTransformer()
    validator   = CaseValidator()
    df_raw      = extractor.extract_cases()
    df_clean    = transformer.transform(df_raw)
    result      = validator.validate_dataframe(df_clean)
    logger.info(f"Validation complete — Pass rate: {result['pass_rate']:.1f}%")
    context['ti'].xcom_push(key='pass_rate', value=result['pass_rate'])
    if result['pass_rate'] < 90.0:
        raise ValueError(
            f"Validation pass rate {result['pass_rate']:.1f}% below 90% threshold"
        )


def task_load(**context):
    from project_1_case_flow_etl.extractors.court_extractor import CourtDataExtractor
    from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer
    from project_1_case_flow_etl.validators.case_validator import CaseValidator
    from project_1_case_flow_etl.loaders.case_loader import CaseLoader
    extractor   = CourtDataExtractor(DATABASE_URL)
    transformer = CaseTransformer()
    validator   = CaseValidator()
    loader      = CaseLoader(DATABASE_URL)
    df_raw      = extractor.extract_cases()
    df_clean    = transformer.transform(df_raw)
    result      = validator.validate_dataframe(df_clean)
    loaded      = loader.load(result['valid_df'], 'clean_cases')
    logger.info(f"Load complete — {loaded:,} rows upserted into clean_cases")
    context['ti'].xcom_push(key='loaded_count', value=loaded)


def task_quality_check(**context):
    from project_3_data_quality.run_checks import run
    results = run()
    passed  = sum(1 for r in results if r['success'])
    total   = len(results)
    logger.info(f"Quality check complete — {passed}/{total} checks passed")
    if passed < total:
        raise ValueError(f"Quality checks failed: {passed}/{total} passed")


def task_reprocess(**context):
    import glob
    import json
    from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer
    from project_1_case_flow_etl.validators.case_validator import CaseValidator
    from project_1_case_flow_etl.loaders.case_loader import CaseLoader
    from project_2_data_migration.reprocessing.reprocessor import IterativeReprocessor

    run_date   = datetime.now().strftime('%Y%m%d')
    error_files = glob.glob(f'migration_errors/raw_cases_{run_date}*.json')

    if not error_files:
        logger.info("No failed records to reprocess")
        return

    reprocessor = IterativeReprocessor()
    transformer = CaseTransformer()
    validator   = CaseValidator()
    loader      = CaseLoader(DATABASE_URL)

    def fix_nulls(df):
        df['case_type'] = df['case_type'].fillna('civil')
        df['status']    = df['status'].fillna('pending')
        return df

    for error_file in error_files:
        reprocessor.reprocess(
            error_file=error_file,
            fix_fn=fix_nulls,
            transformer=transformer,
            validator=validator,
            loader=loader,
            table='clean_cases'
        )
        logger.info(f"Reprocessed: {error_file}")


# ── DAG DEFINITION ──────────────────────────────────────────────────────────

with DAG(
    dag_id='court_data_migration',
    default_args=default_args,
    description='Court case data migration pipeline',
    schedule_interval='0 2 * * *',   # Run nightly at 2am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['justice', 'migration', 'court'],
) as dag:

    extract = PythonOperator(
        task_id='extract_cases',
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id='transform_cases',
        python_callable=task_transform,
    )

    validate = PythonOperator(
        task_id='validate_cases',
        python_callable=task_validate,
    )

    load = PythonOperator(
        task_id='load_cases',
        python_callable=task_load,
    )

    quality = PythonOperator(
        task_id='quality_check',
        python_callable=task_quality_check,
    )

    reprocess = PythonOperator(
        task_id='reprocess_failures',
        python_callable=task_reprocess,
        trigger_rule='all_done',   # Run even if prior task failed
    )

    extract >> transform >> validate >> load >> quality >> reprocess
