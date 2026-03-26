# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

InsureYours is a healthcare data analytics project that identifies the most cost-effective insurance provider for patients based on demographics (age group, blood type), medical condition, and medication. It is a **MySQL + Python data pipeline with a REST API** — not a web application.

The project is designed to demonstrate both Data Engineering and Data Analyst skills.

## Architecture

```
CSV → ETL (src/insureyours/etl_load.py) → MySQL → SQL Analytics → REST API (src/insureyours/api.py)
                                                  → Data Profiler (src/insureyours/data_profiler.py)
                                                  → Statistical Analysis (src/insureyours/statistical_analysis.py)
```

### Pipeline stages:
1. **Schema** (`sql/01_schema.sql`) — Database, staging table, 5 dimension tables, Admission fact table, output table. Idempotent.
2. **ETL** (`src/insureyours/etl_load.py`) — Validates rows, loads staging, populates dimensions, resolves FKs via 5-way JOIN.
3. **Core analysis** (`sql/02_procedures.sql` + `sql/03_analysis.sql`) — View with age groups + LengthOfStay, 3 stored procedures, DENSE_RANK provider ranking.
4. **Advanced analysis** (`sql/04_advanced.sql`) — Percentiles, outlier detection, market share, cost efficiency, readmission proxy, monthly trends.
5. **API** (`src/insureyours/api.py`) — FastAPI REST endpoints serving recommendations and analytics.
6. **Profiling** (`src/insureyours/data_profiler.py`) — 9-section data quality report with automated scoring.
7. **Statistics** (`src/insureyours/statistical_analysis.py`) — Welch's t-test, ANOVA, confidence intervals, Cohen's d.

## Development Commands

```bash
make help                          # Show all commands
make test                          # 54 unit tests (no MySQL needed)
make pipeline MYSQL_PASSWORD=pw    # Full pipeline (schema + ETL + analysis)
make profile  MYSQL_PASSWORD=pw    # Data quality report
make stats    MYSQL_PASSWORD=pw    # Statistical analysis
make api      MYSQL_PASSWORD=pw    # Start REST API on :8000
make docker                        # Full pipeline + API in Docker
docker compose down -v             # Reset Docker environment
```

## Key SQL Objects

- **`HealthCare_Dataset`** — Staging table mirroring CSV. All analytics query here via the view.
- **`vw_PatientAgeGroups`** — Adds AgeGroup and LengthOfStay. Excludes PII. All queries depend on this.
- **`Health_Data_Analysis`** — Output table: cheapest provider per demographic group.

## API Endpoints (src/insureyours/api.py)

- `GET /recommend?age=&condition=&blood_type=&medication=` — Best insurer for a patient
- `GET /analytics/billing-summary` — Avg billing by demographics
- `GET /analytics/provider-compare?condition=` — Side-by-side provider costs
- `GET /conditions` and `GET /insurers` — List available values
- `GET /health` — DB connectivity check
- `GET /docs` — Swagger UI

## Prerequisites

- MySQL 8.0+, Python 3.8+
- `pip install -r requirements.txt` (mysql-connector-python, fastapi, uvicorn, scipy)
- Or just Docker: `docker compose up --build`

## Conventions

### SQL
- `INT AUTO_INCREMENT` PKs, `DECIMAL(18,4)` for billing (not FLOAT)
- CHECK constraints on all categorical columns
- All scripts idempotent (DROP IF EXISTS, TRUNCATE + INSERT)
- `ROUND(..., 2)` on all monetary aggregates
- `DENSE_RANK()` for rankings (preserves ties)

### Python
- Row-level validation before any DB insert
- Batch inserts (1000 rows) via `executemany`
- CLI args for all connection params — no hardcoded values
- `mysql.connector` import is non-fatal (tests run without MySQL)

## Testing

- `tests/test_etl.py` — 27 unit tests covering all ETL validation logic
- `tests/test_api.py` — 25 API endpoint tests (mocked DB, no MySQL needed)
- CI: unit tests + full MySQL integration test via GitHub Actions

## Docker

`docker compose up --build` runs 4 services:
1. `mysql` — database + schema init
2. `etl` — loads data
3. `analysis` — runs all SQL analytics
4. `api` — FastAPI on port 8000

## Legacy

`ETL/` directory has the original Windows SSIS package (SQL Server + Visual Studio). Replaced by `src/insureyours/etl_load.py`.
