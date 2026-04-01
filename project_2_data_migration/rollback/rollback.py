import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logger = logging.getLogger(__name__)

class MigrationRollback:

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def rollback_table(self, table: str):
        logger.warning(f"ROLLBACK initiated for {table}")
        with self.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
            conn.commit()
        logger.warning(f"ROLLBACK complete — {table} truncated")

    def create_snapshot(self, table: str):
        snapshot = f"{table}_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE TABLE {snapshot} AS SELECT * FROM {table}"))
            conn.commit()
        logger.info(f"Snapshot created: {snapshot}")
        return snapshot

    def rollback_to_snapshot(self, table: str, snapshot_table: str):
        logger.warning(f"ROLLBACK — restoring {table} from {snapshot_table}")
        with self.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
            conn.execute(text(f"INSERT INTO {table} SELECT * FROM {snapshot_table}"))
            conn.commit()
        logger.info(f"Restored {table} from {snapshot_table}")
