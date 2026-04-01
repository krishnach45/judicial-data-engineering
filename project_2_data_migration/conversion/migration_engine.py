import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class MigrationEngine:
    """
    Full lifecycle: extract → transform → validate → load → reconcile.
    Every run is logged for auditability.
    Supports iterative reprocessing of failed records.
    """

    def __init__(self, source_engine, target_engine, run_id: str):
        self.source  = source_engine
        self.target  = target_engine
        self.run_id  = run_id
        self.log     = []
        self.error_dir = Path('migration_errors')
        self.error_dir.mkdir(exist_ok=True)

    def run(self, table: str, transformer, validator, loader):
        start = datetime.now()
        self._log(f"Starting migration of {table} — Run: {self.run_id}")

        # Phase 1 — Extract
        df_raw = pd.read_sql(f"SELECT * FROM {table}", self.source)
        self._log(f"Extracted {len(df_raw):,} rows from {table}")

        # Phase 2 — Transform
        df_clean = transformer.transform(df_raw)
        self._log(f"Transformed: {len(df_clean):,} rows remaining")

        # Phase 3 — Validate
        result = validator.validate_dataframe(df_clean)
        self._log(f"Validation pass rate: {result['pass_rate']:.1f}%")

        # Save errors for reprocessing
        if result['errors']:
            self._save_errors(result['errors'], table)
            self._log(f"Saved {result['invalid_count']:,} errors for reprocessing")

        # Phase 4 — Load valid rows only
        loaded = loader.load(result['valid_df'], "clean_cases")
        self._log(f"Loaded {loaded:,} rows into clean_{table}")

        # Phase 5 — Reconcile
        self._reconcile(df_raw, "clean_cases")

        # Save run log
        duration = (datetime.now() - start).seconds
        self._log(f"Migration complete in {duration}s")
        self._save_run_log(table, duration, result)

        return result

    def _reconcile(self, source_df: pd.DataFrame, target_table: str):
        """Compare source and target row counts"""
        try:
            with self.target.connect() as conn:
                target_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {target_table}")
                ).scalar()
            source_count = len(source_df)
            match = "✅" if source_count == target_count else "⚠️"
            self._log(
                f"{match} Reconciliation — "
                f"Source: {source_count:,} | Target: {target_count:,}"
            )
        except Exception as e:
            self._log(f"Reconciliation skipped: {e}")

    def _save_errors(self, errors: list, table: str):
        error_file = self.error_dir / f"{table}_{self.run_id}.json"
        with open(error_file, 'w') as f:
            json.dump(errors, f, indent=2, default=str)
        logger.info(f"Errors saved to {error_file}")

    def _save_run_log(self, table: str, duration: int, result: dict):
        log_file = self.error_dir / f"run_log_{self.run_id}.json"
        log_data = {
            'run_id':        self.run_id,
            'table':         table,
            'duration_secs': duration,
            'valid_count':   result['valid_count'],
            'invalid_count': result['invalid_count'],
            'pass_rate':     result['pass_rate'],
            'log':           self.log
        }
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2, default=str)
        logger.info(f"Run log saved to {log_file}")

    def _log(self, msg: str):
        ts = datetime.now().isoformat()
        logger.info(msg)
        self.log.append({'ts': ts, 'msg': msg})