import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import json
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://court_admin:court_pass@127.0.0.1:5433/court_dw"
)

VALID_CASE_TYPES = {"criminal", "civil", "family", "traffic", "probate"}
VALID_STATUSES   = {"open", "closed", "pending", "dismissed", "appealed"}

def run():
    logger.info("Starting data quality checks...")
    engine = create_engine(DATABASE_URL)
    df     = pd.read_sql("SELECT * FROM clean_cases", engine)
    logger.info(f"Loaded {len(df):,} rows from clean_cases")

    results = []

    def check(name, column, passed, total, threshold=1.0):
        pass_rate = passed / total if total > 0 else 0
        success   = pass_rate >= threshold
        status    = "✅ PASS" if success else "❌ FAIL"
        results.append({
            "check":     name,
            "column":    column,
            "passed":    passed,
            "total":     total,
            "pass_rate": round(pass_rate * 100, 2),
            "threshold": threshold * 100,
            "success":   success
        })
        logger.info(f"  {status} | {name} | {column} | {passed:,}/{total:,} ({pass_rate*100:.1f}%)")
        return success

    total = len(df)
    logger.info("─" * 60)
    logger.info("DATA QUALITY RESULTS")
    logger.info("─" * 60)

    check("not_null",     "case_number", int(df["case_number"].notna().sum()), total)
    check("unique",       "case_number", int(df["case_number"].nunique()), total)
    check("valid_values", "case_type",
          int((df["case_type"].isin(VALID_CASE_TYPES) | df["case_type"].isna()).sum()),
          total, threshold=0.95)
    check("valid_values", "status",
          int((df["status"].isin(VALID_STATUSES) | df["status"].isna()).sum()),
          total, threshold=0.95)
    check("not_null",     "status",   int(df["status"].notna().sum()),   total, threshold=0.95)
    check("not_null",     "court_id", int(df["court_id"].notna().sum()), total)

    ssn_removed = "ssn" not in df.columns
    status = "✅ PASS" if ssn_removed else "❌ FAIL"
    logger.info(f"  {status} | pii_removed | ssn | SSN column removed: {ssn_removed}")
    results.append({"check": "pii_removed", "column": "ssn",
                    "passed": 1 if ssn_removed else 0, "total": 1,
                    "pass_rate": 100 if ssn_removed else 0,
                    "threshold": 100, "success": ssn_removed})

    status = "✅ PASS" if total > 0 else "❌ FAIL"
    logger.info(f"  {status} | row_count_gt_0 | table | Rows: {total:,}")
    results.append({"check": "row_count_gt_0", "column": "table",
                    "passed": total, "total": total,
                    "pass_rate": 100, "threshold": 100, "success": total > 0})

    passed_checks = sum(1 for r in results if r["success"])
    total_checks  = len(results)
    overall       = passed_checks == total_checks

    logger.info("─" * 60)
    logger.info(f"Overall: {'✅ ALL PASSED' if overall else '❌ SOME FAILED'}")
    logger.info(f"Passed:  {passed_checks}/{total_checks} checks")
    logger.info("─" * 60)

    report_dir  = Path("project_3_data_quality/reports")
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, "w") as f:
        json.dump({
            "run_at":        datetime.now().isoformat(),
            "table":         "clean_cases",
            "total_rows":    total,
            "checks_passed": passed_checks,
            "checks_total":  total_checks,
            "overall":       overall,
            "results":       results
        }, f, indent=2)
    logger.info(f"Report saved to {report_file}")
    return results

if __name__ == "__main__":
    run()
