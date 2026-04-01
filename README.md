# Judicial Data Engineering Portfolio

> End-to-end data engineering system for court case management — built to mirror real justice software implementations.

![CI](https://github.com/krishnach45/judicial-data-engineering/actions/workflows/ci.yml/badge.svg)
![Data Quality](https://github.com/krishnach45/judicial-data-engineering/actions/workflows/data_quality_check.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Overview

This portfolio demonstrates the complete data conversion lifecycle used in justice software implementations — from raw court system exports through transformation, validation, iterative reprocessing, and analytics-ready data warehouse modeling.

Built specifically to reflect the data engineering patterns used in court, attorney, and jury case management systems including Tyler Technologies Odyssey-style schemas, PII-safe data handling, and production-grade pipeline repeatability.

---

## Projects

| # | Project | Description | Key Skills |
|---|---------|-------------|------------|
| 1 | Court Case ETL Pipeline | Extract, transform, validate, and load court case records | ETL, PII hashing, Pydantic validation, idempotent upserts |
| 2 | Data Migration Lifecycle Engine | Full migration cycle with iterative reprocessing | Sampling, conversion, reprocessing, reconciliation |
| 3 | Automated Data Quality Framework | Rule-based validation with audit reports | Data quality, automated checks, JSON reporting |
| 4 | Airflow Orchestration | Nightly pipeline scheduling with retry logic | DAGs, SLA monitoring, task dependencies |
| 5 | CI/CD Pipeline | Automated testing on every commit | GitHub Actions, pytest, PostgreSQL integration tests |
| 6 | dbt Data Warehouse | Analytics-ready court data models | dbt, SQL transformations, schema tests, documentation |

---

## Tech Stack

| Tool | Purpose | Why This Tool |
|------|---------|---------------|
| Python 3.11+ | ETL scripting | Industry standard for data transformation; rich judicial data ecosystem |
| PostgreSQL 15 | Primary database | Used by Tyler Technologies, Odyssey, and C-Track court systems |
| Apache Airflow | Pipeline orchestration | Gold standard for iterative reprocessing workflows |
| dbt | SQL transformations | Modern standard for versioned, tested, documented analytics models |
| Docker | Environment management | Ensures pipeline reproducibility across dev and production |
| Pydantic | Data validation | Schema enforcement before data reaches the target database |
| Pandas + SQLAlchemy | Data manipulation | Handles complex deduplication, fuzzy matching, and DB abstraction |
| GitHub Actions | CI/CD | Automated testing on every commit — proves long-term repeatability |
| Great Expectations | Data quality | Industry-standard validation with stakeholder-facing HTML reports |

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/krishnach45/judicial-data-engineering.git
cd judicial-data-engineering

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL
docker-compose up -d

# Generate synthetic court data (10,000 cases with intentional quality issues)
python data/synthetic/generate_data.py

# Seed raw tables
python loaders/seed_raw_tables.py

# Run Project 1 — ETL Pipeline
python -m project_1_case_flow_etl.run

# Run Project 2 — Full Migration Lifecycle
python -m project_2_data_migration.run

# Run Project 3 — Data Quality Checks
python project_3_data_quality/run_checks.py

# Run all tests
pytest project_1_case_flow_etl/tests/ -v
```

---

## Repository Structure

```
judicial-data-engineering/
│
├── data/
│   └── synthetic/
│       └── generate_data.py          ← Synthetic court data generator
│
├── loaders/
│   └── seed_raw_tables.py            ← Seed raw PostgreSQL tables
│
├── project_1_case_flow_etl/          ← Court Case ETL Pipeline
│   ├── extractors/court_extractor.py
│   ├── transformers/case_transformer.py
│   ├── validators/case_validator.py
│   ├── loaders/case_loader.py
│   ├── tests/
│   │   ├── test_transformer.py       ← 7 unit tests
│   │   └── test_validator.py         ← 5 unit tests
│   └── run.py
│
├── project_2_data_migration/         ← Data Migration Lifecycle Engine
│   ├── sampling/sampler.py
│   ├── conversion/migration_engine.py
│   ├── reprocessing/reprocessor.py
│   ├── rollback/rollback.py
│   └── run.py
│
├── project_3_data_quality/           ← Data Quality Framework
│   ├── expectations/
│   ├── checkpoints/
│   ├── reports/
│   └── run_checks.py
│
├── project_4_airflow_orchestration/  ← Airflow DAGs
│   └── dags/court_migration_dag.py
│
├── project_5_dbt_warehouse/          ← dbt Data Warehouse
│   ├── models/
│   │   ├── staging/stg_cases.sql
│   │   ├── intermediate/int_case_summary.sql
│   │   └── marts/
│   │       ├── case_analytics.sql
│   │       └── judge_metrics.sql
│   └── dbt_project.yml
│
├── .github/
│   └── workflows/
│       ├── ci.yml                    ← GitHub Actions CI/CD
│       └── data_quality_check.yml   ← Nightly data quality workflow
│
├── docker-compose.yml
└── requirements.txt
```

---

## Project 1 — Court Case ETL Pipeline

Implements the full extract-transform-load cycle for court case records, modeled on real justice software data structures.

Schema covers: cases, parties, hearings, charges, verdicts, judges

**Key capabilities:**
- Extracts from `raw_cases` PostgreSQL table (seeded from CSV exports)
- Normalizes inconsistent case number formats (`CR/2024/001`, `cr-2024-001`, `CRIM 2024 001` → `CR-2024-001`)
- Handles 6 different legacy date formats from source systems
- SHA-256 hashes all SSN fields — raw PII never persists in the target database
- Removes ~3% duplicate case records introduced by legacy system re-entry
- Validates every record with Pydantic schema enforcement
- Saves failed records to `migration_errors/` for iterative reprocessing
- Idempotent upsert loading — safe to run multiple times without creating duplicates

**Sample run output:**
```
2024-01-15 02:01:12 — INFO — Starting ETL pipeline — Run ID: 20240115_020112
2024-01-15 02:01:15 — INFO — Extracted 10,000 rows from raw_cases
2024-01-15 02:01:18 — INFO — Case numbers normalized
2024-01-15 02:01:19 — WARNING — 7,512 unparseable dates in 'filed_date' set to NULL
2024-01-15 02:01:20 — INFO — SSN column hashed and removed
2024-01-15 02:01:21 — INFO — Removed 247 duplicate case records
2024-01-15 02:01:22 — INFO — Transformation complete: 9,753 rows remaining
2024-01-15 02:01:25 — INFO — Validation: 9,195 passed | 558 failed | Pass rate: 94.3%
2024-01-15 02:01:26 — INFO — Saved 558 errors to migration_errors/
2024-01-15 02:01:28 — INFO — Upserted 9,195 rows into clean_cases
```

---

## Project 2 — Data Migration Lifecycle Engine

Implements the full conversion lifecycle required in justice software migrations — the same workflow used in Odyssey and Tyler Technologies implementations.

**Phases:**
1. **Sampling** — Stratified 10% sample by case type for pre-migration profiling
2. **Profiling** — Null rate analysis, type detection, anomaly flagging
3. **Transformation** — Full cleaning and standardization
4. **Loading** — Idempotent upsert with reconciliation
5. **Iterative Reprocessing** — Failed records isolated, fix applied, reloaded without restarting

> Justice software migrations run in cycles — not as single batch jobs. Records that fail validation in one pass get isolated, fixed with targeted scripts, and reloaded. This engine supports that pattern natively.

```
Phase 1 — Sampling:   998 rows sampled (10% of 10,000)
Phase 2 — Migration:  9,195 valid | 558 failed | 94.3% pass rate
Phase 3 — Reprocess:  412 recovered | 146 still failing
```

---

## Project 3 — Automated Data Quality Framework

Rule-based validation suite that runs against `clean_cases` after every migration cycle.

| Check | Column | Result |
|-------|--------|--------|
| Not null | case_number | ✅ PASS |
| Unique | case_number | ✅ PASS |
| Valid values | case_type | ✅ PASS |
| Valid values | status | ✅ PASS |
| Not null | status | ✅ PASS |
| Not null | court_id | ✅ PASS |
| PII removed | ssn | ✅ PASS |
| Row count > 0 | table | ✅ PASS |

Reports saved to `project_3_data_quality/reports/` as timestamped JSON files.

---

## Project 4 — Airflow Orchestration

Nightly DAG that automates the full pipeline with production-grade reliability features.

- **Schedule:** `0 2 * * *` (nightly at 2am)
- **Retries:** 3 attempts with 5-minute delay
- **SLA:** 2-hour alert threshold
- **Task flow:** `extract → transform → validate → load → quality_check → reprocess`
- `reprocess` task uses `trigger_rule='all_done'` — runs even if prior tasks fail

> Note: Airflow requires Linux/WSL2. The DAG code is fully implemented and syntax-validated. Deploy to any Linux Airflow instance or cloud (MWAA, Cloud Composer, Astronomer).

---

## Project 5 — CI/CD Pipeline

GitHub Actions workflow that runs on every push to `main` and `develop`.

**Pipeline steps:**
1. Spin up PostgreSQL 15 service container
2. Install all Python dependencies
3. Run 12 unit tests against transformer and validator
4. Validate Airflow DAG syntax
5. Run data quality checks

**Test results: 12/12 passing**

```
test_transformer.py::test_cleans_case_numbers      PASSED
test_transformer.py::test_hashes_ssn               PASSED
test_transformer.py::test_removes_duplicates        PASSED
test_transformer.py::test_standardizes_case_type   PASSED
test_transformer.py::test_null_case_number         PASSED
test_transformer.py::test_drops_system_columns     PASSED
test_transformer.py::test_full_transform_pipeline  PASSED
test_validator.py::test_valid_record_passes        PASSED
test_validator.py::test_invalid_case_number_fails  PASSED
test_validator.py::test_invalid_case_type_fails    PASSED
test_validator.py::test_null_case_type_passes      PASSED
test_validator.py::test_multiple_records_mixed     PASSED
```

---

## Project 6 — dbt Data Warehouse

Analytics-ready SQL models with built-in tests and documentation.

```
staging/
  stg_cases.sql             ← Type casting, renaming, null filtering

intermediate/
  int_case_summary.sql      ← Case counts by type, status, and court

marts/
  case_analytics.sql        ← Final analytics table with derived fields
  judge_metrics.sql         ← Per-judge caseload and closure rates
```

**Derived fields in `case_analytics`:**
- `resolution_category` — active vs resolved
- `court_division` — criminal justice, civil justice, family court
- `filing_year`, `filing_month`, `filing_quarter`

---

## Data Generation

No external API or real court data needed. The synthetic generator creates realistic court records with intentional data quality problems that mirror what you find in legacy court system exports.

```bash
python data/synthetic/generate_data.py
```

| Table | Rows |
|-------|------|
| Cases | 10,000 |
| Parties | ~25,000 |
| Hearings | ~30,000 |
| Charges | ~5,900 |
| Judges | 50 |

**Intentional quality problems injected:**

| Problem | Why It Exists in Real Systems |
|---------|-------------------------------|
| Mixed date formats | Different clerks export dates differently across counties |
| ~3% duplicate case numbers | Legacy systems allow re-entry under slightly different numbers |
| Inconsistent prefixes (CR, cr, CRIM, blank) | Abbreviation conventions changed over decades |
| 5–20% null rates on optional fields | Historical records pre-date mandatory field requirements |
| Raw SSNs in source data | Legacy systems stored PII in plain text |
| Future hearing dates mixed with past | Scheduled future hearings coexist with historical records |

---

## Alignment with Justice Software Requirements

| Requirement | How This Project Covers It |
|-------------|---------------------------|
| Justice software implementation (court, attorney, jury) | Court case schema, party/attorney relationships, hearing scheduling |
| Data hygiene and cleaning scripts | `case_transformer.py` — normalization, deduplication, PII hashing |
| Scripts for data migration and conversion | `migration_engine.py` — full lifecycle with run logs |
| Sampling, transforming, loading, iterative reprocessing | `sampler.py`, `migration_engine.py`, `reprocessor.py` |
| Clean, accurate, ready-to-use data | Pydantic validation + Great Expectations quality gates |
| Strong programmer with automation mindset | Airflow DAGs replace all manual execution |
| System reliability and long-term repeatability | CI/CD ensures every commit is tested before deployment |

---

## License

MIT License — see [LICENSE](LICENSE)

---

> Built as a targeted portfolio project for justice software data engineering roles. Every component maps directly to production patterns used in court case management system implementations.
