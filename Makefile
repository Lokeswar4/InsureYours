.PHONY: help setup etl analyze advanced profile stats api test lint format docker docker-down docker-reset pipeline clean

# Default MySQL connection (override with: make etl MYSQL_HOST=10.0.0.5)
MYSQL_HOST     ?= localhost
MYSQL_PORT     ?= 3306
MYSQL_USER     ?= root
MYSQL_PASSWORD ?=
MYSQL_DB       = Healthcare_Group_Project

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ---------- Pipeline (requires MySQL + Python) ----------

setup: ## Create database and tables
	mysql -h $(MYSQL_HOST) -P $(MYSQL_PORT) -u $(MYSQL_USER) -p$(MYSQL_PASSWORD) < Tables_Creation.sql

etl: ## Run ETL pipeline (load CSV into MySQL)
	uv run python etl_load.py \
		--host $(MYSQL_HOST) --port $(MYSQL_PORT) \
		--user $(MYSQL_USER) --password $(MYSQL_PASSWORD)

analyze: ## Create procedures and run core + advanced analysis
	mysql -h $(MYSQL_HOST) -P $(MYSQL_PORT) -u $(MYSQL_USER) -p$(MYSQL_PASSWORD) $(MYSQL_DB) < StoredProcedure.sql
	mysql -h $(MYSQL_HOST) -P $(MYSQL_PORT) -u $(MYSQL_USER) -p$(MYSQL_PASSWORD) $(MYSQL_DB) < Analysis.sql
	mysql -h $(MYSQL_HOST) -P $(MYSQL_PORT) -u $(MYSQL_USER) -p$(MYSQL_PASSWORD) $(MYSQL_DB) < Advanced_Analysis.sql

pipeline: setup etl analyze ## Run full pipeline end-to-end

# ---------- Analysis Tools (Data Analyst) ----------

profile: ## Generate data quality & profiling report
	uv run python data_profiler.py \
		--host $(MYSQL_HOST) --port $(MYSQL_PORT) \
		--user $(MYSQL_USER) --password $(MYSQL_PASSWORD)

stats: ## Run statistical analysis (t-tests, ANOVA, CIs)
	uv run python statistical_analysis.py \
		--host $(MYSQL_HOST) --port $(MYSQL_PORT) \
		--user $(MYSQL_USER) --password $(MYSQL_PASSWORD)

# ---------- API (Data Engineering) ----------

api: ## Start the REST API server on port 8000
	uv run python api.py \
		--db-host $(MYSQL_HOST) --db-port $(MYSQL_PORT) \
		--db-user $(MYSQL_USER) --db-password $(MYSQL_PASSWORD)

# ---------- Quality ----------

test: ## Run Python tests
	uv run pytest tests/ -v

lint: ## Run ruff linter
	uv run ruff check .

format: ## Format code with ruff
	uv run ruff format .

check: lint test ## Run lint + tests

# ---------- Docker ----------

docker: ## Run full pipeline + API in Docker
	docker compose up --build

docker-down: ## Stop and remove containers
	docker compose down

docker-reset: ## Stop containers and delete database volume
	docker compose down -v

# ---------- Utilities ----------

clean: ## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name '*.pyc' -delete 2>/dev/null; true
