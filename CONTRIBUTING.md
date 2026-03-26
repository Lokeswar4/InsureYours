# Contributing

## Architecture

```
CSV → ETL (src/insureyours/etl_load.py) → MySQL → SQL Analytics → REST API (src/insureyours/api.py)
                                                  → Data Profiler (src/insureyours/data_profiler.py)
                                                  → Statistical Analysis (src/insureyours/statistical_analysis.py)
```

### Pipeline stages

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
- CI: lint + unit tests + full MySQL integration test via GitHub Actions

Run tests:

```bash
make test
```

## Legacy

`ETL/` directory contains the original Windows SSIS package (SQL Server + Visual Studio), kept for reference.
