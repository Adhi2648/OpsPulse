# OpsPulse Analytics Platform

OpsPulse is an operations intelligence portfolio project built with Python, SQL, PostgreSQL, FastAPI, Airflow, and Tableau. The platform is designed to ingest 500K+ workflow records, validate and clean them, compute operational KPIs, detect exceptions, refresh data daily, and expose results through APIs and dashboards.

This repository is being built iteratively. The current milestone includes:

- production-style folder structure
- `pyproject.toml`
- Dockerized local infrastructure
- PostgreSQL warehouse schema
- synthetic data generator for 500K workflow records
- local ETL pipeline modules
- validation and quarantine handling
- initial pytest coverage for validation and transform logic

## Current Structure

```text
opspulse/
├── config/
├── dags/
├── data/
├── docs/
├── logs/
├── scripts/
│   └── generate_workflow_data.py
├── sql/
│   ├── init/
│   │   ├── 001_create_schemas.sql
│   │   └── 002_create_tables.sql
│   └── marts/
│       └── 001_reporting_views.sql
├── src/
│   └── opspulse/
│       ├── api/
│       ├── core/
│       ├── db/
│       ├── etl/
│       └── models/
├── tests/
├── .env.example
├── docker-compose.yml
└── pyproject.toml
```

## Quick Start

### 1. Copy environment variables

```bash
cp .env.example .env
```

Fill in at least:

- `POSTGRES_PASSWORD`
- `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW_FERNET_KEY`

Example Fernet key generation:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Install the package for local module execution

```bash
python -m pip install -e .
```

### 3. Start PostgreSQL and Airflow

```bash
docker compose up -d --build postgres airflow-init airflow-scheduler airflow-webserver
```

### 4. Generate synthetic source data

```bash
python scripts/generate_workflow_data.py --records 500000
```

Generated files are written under `data/raw/`.

### 5. Run ETL locally

```bash
python -m opspulse.etl.pipeline --input data/raw/workflow_events.csv --dry-run
python -m opspulse.etl.pipeline --input data/raw/workflow_events.csv
```

### 6. Run tests

```bash
python -m pytest tests
```

To run integration tests only:

```bash
python -m pytest tests/integration -m integration
```

## Airflow Verification

### Trigger the DAG manually

Open Airflow at `http://localhost:8080`, sign in with the admin credentials from your local `.env`, then trigger `opspulse_daily_pipeline`.

You can optionally override the input file path with DAG run configuration:

```json
{
  "input_path": "/opt/airflow/data/raw/workflow_events.csv"
}
```

The DAG is organized into these task groups:

- input readiness check
- extract
- validate
- load raw
- transform and load warehouse
- data quality summary

### Inspect populated warehouse tables

Using `psql` locally:

```bash
psql -h localhost -U opspulse -d opspulse -c "SELECT COUNT(*) FROM raw.workflow_events_raw;"
psql -h localhost -U opspulse -d opspulse -c "SELECT COUNT(*) FROM staging.workflow_events_quarantine;"
psql -h localhost -U opspulse -d opspulse -c "SELECT COUNT(*) FROM warehouse.fact_workflow_run;"
psql -h localhost -U opspulse -d opspulse -c "SELECT COUNT(*) FROM warehouse.fact_exception;"
psql -h localhost -U opspulse -d opspulse -c "SELECT COUNT(*) FROM marts.kpi_daily_summary;"
```

From Docker:

```bash
docker exec -it opspulse-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

## Warehouse Layout

- `raw`: immutable landing tables for source workflow events
- `staging`: validated and standardized workflow records
- `warehouse`: fact and dimension tables for analytics
- `marts`: KPI summaries and reporting views

See `docs/architecture.md` for the system design and `sql/init/` for the database schema.
