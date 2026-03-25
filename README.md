# InsureYours

[![CI](https://github.com/Lokeswar4/InsureYours/actions/workflows/ci.yml/badge.svg)](https://github.com/Lokeswar4/InsureYours/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![MySQL 8.0+](https://img.shields.io/badge/MySQL-8.0%2B-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)

> Find the most cost-effective insurance provider based on patient demographics, medical conditions, and medication needs.

A healthcare data analytics pipeline that processes 10,000 patient records through a validated ETL, normalizes the data, ranks insurance providers by average billing cost, validates findings with statistical tests, and serves recommendations via a REST API.

---

## Quick Start (Docker — recommended)

```bash
git clone https://github.com/Lokeswar4/InsureYours.git
cd InsureYours

# Download the dataset from Kaggle and place it in the project root:
# https://www.kaggle.com/datasets/prasad22/healthcare-dataset
# Save as: healthcare_dataset.csv

# Run the entire pipeline + API with one command:
docker compose up --build
```

This starts MySQL, runs the ETL, executes all analytics, and starts the API at **http://localhost:8000/docs**.

Open the interactive dashboard at **http://localhost:8000/dashboard**.

Try a recommendation:
```
http://localhost:8000/recommend?age=45&condition=Diabetes&blood_type=O%2B&medication=Aspirin
```

---

## Skills Demonstrated

This project is designed to showcase both **Data Engineering** and **Data Analyst** competencies:

### Data Engineering

| Skill | Where |
|-------|-------|
| ETL pipeline design | `etl_load.py` — CSV validation, staging, FK resolution |
| Database schema design | `Tables_Creation.sql` — normalized star schema with constraints |
| REST API development | `api.py` — FastAPI with connection pooling, Swagger docs |
| Docker containerization | `docker-compose.yml` — multi-service orchestration |
| CI/CD pipeline | `.github/workflows/ci.yml` — unit + integration tests |
| Data quality validation | `data_profiler.py` — automated quality checks with scoring |
| Idempotent scripts | All SQL and ETL scripts are safe to re-run |

### Data Analyst

| Skill | Where |
|-------|-------|
| Exploratory Data Analysis | `notebooks/exploratory_analysis.ipynb` — distributions, trends, heatmaps |
| EDA (automated report) | `data_profiler.py` — cardinality, percentiles, outliers, quality score |
| Statistical hypothesis testing | `statistical_analysis.py` — Welch's t-test, ANOVA, Cohen's d |
| Confidence intervals | `statistical_analysis.py` — 95% CIs for provider means |
| Window functions | `Advanced_Analysis.sql` — DENSE_RANK, ROW_NUMBER, LAG, running totals |
| Business insight generation | `Analysis.sql` — actionable "cheapest insurer" recommendations |
| Data visualization prep | Stored procedures + Power BI dashboard |

---

## Architecture

```
  healthcare_dataset.csv
          |
    [etl_load.py]        ← Data Engineering: validated ETL
          |
  HealthCare_Dataset     ← Staging table
          |
    +-----+------+
    |            |
  Normalized   vw_PatientAgeGroups   ← Analytical view
  Tables              |
                +-----+------+------+
                |            |      |
          Procedures   Analysis  Advanced    ← Data Analyst: SQL analytics
                |            |      |
          Health_Data_Analysis      |
                |                   |
           [api.py]                 |        ← Data Engineering: REST API
                |                   |
    [data_profiler.py]  [statistical_analysis.py]   ← Data Analyst: EDA + stats
```

## Key Analyses

| Analysis | Tool | What It Answers |
|----------|------|-----------------|
| Billing by demographics | `StoredProcedure.sql` | Which age groups and conditions face highest costs? |
| Provider cost comparison | `StoredProcedure.sql` | How do insurers compare on price? |
| Cost per day of stay | `StoredProcedure.sql` | Is a "cheap" insurer actually cheap, or just shorter stays? |
| Optimal insurer ranking | `Analysis.sql` | Which insurer is cheapest for my exact profile? |
| Billing percentiles | `Advanced_Analysis.sql` | What's the full cost distribution, not just the average? |
| Provider market share | `Advanced_Analysis.sql` | Which insurer dominates each condition? |
| Outlier detection | `Advanced_Analysis.sql` | Which claims are abnormally expensive? |
| Cost efficiency score | `Advanced_Analysis.sql` | Billing per day — true efficiency metric |
| Readmission proxy | `Advanced_Analysis.sql` | Do some insurers have more repeat patients? |
| Monthly trends | `Advanced_Analysis.sql` | Are costs seasonal? |
| Statistical significance | `statistical_analysis.py` | Are cost differences real or random noise? |
| Confidence intervals | `statistical_analysis.py` | How precise are the provider cost estimates? |
| Data quality score | `data_profiler.py` | Is the dataset clean enough to trust? |

## REST API

After running the pipeline, the API serves at **http://localhost:8000**.

| Endpoint | Description |
|----------|-------------|
| `GET /dashboard` | Interactive web dashboard (charts + recommendation form) |
| `GET /docs` | Interactive Swagger documentation |
| `GET /recommend?age=45&condition=Diabetes&blood_type=O+&medication=Aspirin` | Best insurer for a patient |
| `GET /conditions` | List available medical conditions |
| `GET /insurers` | List available insurance providers |
| `GET /analytics/billing-summary?condition=Diabetes` | Avg billing by demographics |
| `GET /analytics/provider-compare?condition=Cancer` | Side-by-side provider costs |
| `GET /analytics/outliers` | Claims >2 StdDev above condition mean |
| `GET /health` | Database connectivity check |

## Dataset

**Source:** [Healthcare Dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) (Kaggle)

10,000 synthetic patient records with 15 attributes:

| Attribute | Example | Used In |
|-----------|---------|---------|
| Age | 45 | Age group bucketing (9 groups) |
| Blood Type | O+ | Insurance ranking partition |
| Medical Condition | Diabetes | All analyses |
| Insurance Provider | Medicare | Cost comparison + ranking |
| Billing Amount | 25,438.00 | Primary metric |
| Admission/Discharge Date | 2023-01-15 | Length of stay calculation |
| Medication | Aspirin | Insurance ranking partition |
| Admission Type | Elective | Cost breakdown |

## Project Structure

```
InsureYours/
├── Tables_Creation.sql           # MySQL schema (staging + normalized + output tables)
├── etl_load.py                   # Python ETL with row-level validation
├── StoredProcedure.sql           # Views and analytical stored procedures
├── Analysis.sql                  # Insurance ranking (DENSE_RANK on AVG)
├── Advanced_Analysis.sql         # Percentiles, outliers, trends, market share
├── api.py                        # FastAPI REST API with Swagger docs + dashboard
├── data_profiler.py              # Data quality & EDA report generator
├── statistical_analysis.py       # Hypothesis tests, CIs, ANOVA, Cohen's d
├── run_pipeline.sh               # One-script full pipeline runner
├── Makefile                      # make pipeline / profile / stats / api / test
├── docker-compose.yml            # One-command Docker setup (DB + ETL + analysis + API)
├── Dockerfile                    # Python container for ETL + API
├── pyproject.toml                # Project config (uv + ruff + pytest)
├── uv.lock                       # Locked dependencies (deterministic installs)
├── requirements.txt              # Fallback for pip users
├── .env.example                  # Environment variable template
├── dashboard/
│   └── index.html                # Interactive web dashboard (Chart.js)
├── notebooks/
│   └── exploratory_analysis.ipynb  # EDA: distributions, trends, heatmaps
├── tests/
│   ├── test_etl.py               # ETL validation unit tests
│   └── test_api.py               # API endpoint tests (mocked DB)
├── .github/
│   └── workflows/ci.yml          # CI: lint + unit tests + MySQL integration test
├── ETL/                          # Legacy SSIS package (Windows, for reference)
├── healthcare_dataset.csv        # Data file (download from Kaggle, gitignored)
└── HealthCare_Final_Project.pbix # Power BI dashboard (gitignored)
```

## Setup Options

### Option 1: Docker Compose (recommended)

```bash
docker compose up --build           # Run pipeline + start API
docker compose down                 # Stop containers
docker compose down -v              # Stop + delete database
```

### Option 2: Local with uv (recommended for development)

```bash
# Install uv (Rust-based Python package manager — fast, deterministic)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all dependencies
uv sync --group dev

# Run the pipeline
make pipeline MYSQL_PASSWORD=yourpassword
```

### Option 3: Local with pip

```bash
# Ubuntu/Debian
sudo apt install mysql-server python3 python3-pip
pip install -r requirements.txt

# macOS
brew install mysql python
pip install -r requirements.txt
```

```bash
# Full pipeline
./run_pipeline.sh localhost root yourpassword

# Or step by step with Make
make pipeline MYSQL_PASSWORD=yourpassword
make profile  MYSQL_PASSWORD=yourpassword   # Data quality report
make stats    MYSQL_PASSWORD=yourpassword   # Statistical analysis
make api      MYSQL_PASSWORD=yourpassword   # Start API server
```

### Makefile Commands

```
  Pipeline        setup / etl / analyze / pipeline (end-to-end)
  Analysis        profile (data quality) / stats (statistical tests)
  API             api (start REST server on :8000)
  Quality         lint (ruff check) / format (ruff format) / check (lint + test)
  Testing         test (27 unit tests)
  Docker          docker / docker-down / docker-reset
```

## Testing

```bash
make test    # Run all unit tests (no MySQL needed)
```

| Test file | Coverage |
|-----------|----------|
| `tests/test_etl.py` | Age/gender/blood type validation, billing, dates, whitespace, CSV schema |
| `tests/test_api.py` | All API endpoints with mocked DB — validation, success, 404, 422 paths |

CI runs lint (ruff), unit tests, and a full MySQL integration test on every push and PR.

### Jupyter Notebook (EDA)

```bash
uv sync --group notebook
jupyter notebook notebooks/exploratory_analysis.ipynb
```

Covers: billing distributions, provider comparison heatmap, length-of-stay analysis, monthly trends, outlier detection.

CI runs both unit tests and a full MySQL integration test on every push and PR.

## Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Make sure tests pass (`make test`)
4. Commit and push
5. Open a Pull Request

## License

[GNU General Public License v3.0](LICENSE)
