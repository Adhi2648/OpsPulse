# OpsPulse Analytics Platform

OpsPulse is a portfolio-ready operations intelligence platform built with Python, SQL, PostgreSQL, FastAPI, Airflow, and a lightweight static demo dashboard. It ingests large workflow datasets, validates and quarantines bad records, loads a warehouse with raw, staging, fact, dimension, and mart layers, exposes analytics through APIs, and orchestrates daily refreshes through Airflow.

This repo is meant to be runnable end-to-end by a recruiter or interviewer:

1. start Docker services
2. generate synthetic workflow data
3. run ETL locally or via Airflow
4. query the API
5. inspect warehouse tables and marts
6. open the lightweight demo dashboard

## Project Overview

OpsPulse demonstrates:

- warehouse-first analytics design
- modular ETL for extract, validate, transform, and load
- quarantine handling for invalid source records
- PostgreSQL reporting marts and views
- daily Airflow orchestration
- FastAPI analytics endpoints
- unit, API, and PostgreSQL integration tests

## Architecture Summary

High-level flow:

1. workflow source CSV lands in `data/raw/`
2. ETL validates source records and quarantines bad rows
3. valid rows load into `raw.workflow_events_raw`
4. transforms populate `staging`, `warehouse`, and `marts`
5. FastAPI reads warehouse facts and reporting views
6. Airflow runs the same ETL modules on a schedule

See `docs/architecture.md` for the detailed system design.

## Tech Stack

- Python 3.11+
- PostgreSQL 16
- SQLAlchemy
- FastAPI
- Apache Airflow
- Pandas
- Pytest
- Docker / Docker Compose

## Repo Structure

```text
opspulse/
├── dags/
│   └── opspulse_daily_pipeline.py
├── data/
│   ├── diagnostics/
│   └── raw/
├── docker/
│   ├── airflow/
│   │   └── Dockerfile
│   └── api/
│       └── Dockerfile
├── docs/
│   ├── architecture.md
│   └── demo/
│       └── index.html
├── scripts/
│   └── generate_workflow_data.py
├── sql/
│   ├── init/
│   │   ├── 001_create_schemas.sql
│   │   ├── 002_create_tables.sql
│   │   └── 003_create_quarantine_table.sql
│   └── marts/
│       └── 001_reporting_views.sql
├── src/
│   └── opspulse/
│       ├── api/
│       ├── core/
│       ├── db/
│       ├── etl/
│       └── utils/
├── tests/
│   ├── fixtures/
│   ├── integration/
│   ├── test_api.py
│   ├── test_transform.py
│   └── test_validate.py
├── .env.example
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## Environment Variables

Copy the example file:

```bash
cp .env.example .env
```

Fill in at least:

- `POSTGRES_PASSWORD`
- `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW_FERNET_KEY`

Useful variables:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `RAW_DATA_DIR`
- `LOG_LEVEL`

Generate a valid Fernet key:

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

## Setup Instructions

### 1. Install local Python package

```bash
python -m pip install -e .
python -m pip install pytest pydantic-settings httpx
```

### 2. Start PostgreSQL, Airflow, and API services

```bash
docker compose up -d --build postgres airflow-init airflow-scheduler airflow-webserver api
```

Service URLs:

- PostgreSQL: `localhost:5432`
- Airflow: `http://localhost:8080`
- FastAPI: `http://localhost:8000`
- FastAPI docs: `http://localhost:8000/docs`

## Generate Sample Data

Generate a 500K-row workflow file:

```bash
python scripts/generate_workflow_data.py --records 500000
```

Outputs:

- `data/raw/workflow_events.csv`
- `data/raw/workflow_events_metadata.json`

## Run ETL Locally

Dry run:

```bash
python -m opspulse.etl.pipeline --input data/raw/workflow_events.csv --dry-run
```

Full load:

```bash
python -m opspulse.etl.pipeline --input data/raw/workflow_events.csv
```

Skip DB writes:

```bash
python -m opspulse.etl.pipeline --input data/raw/workflow_events.csv --skip-load
```

## Trigger the Airflow DAG

DAG name:

- `opspulse_daily_pipeline`

Open Airflow at `http://localhost:8080`, sign in with your `.env` credentials, then unpause and trigger the DAG.

Optional DAG run config:

```json
{
  "input_path": "/opt/airflow/data/raw/workflow_events.csv"
}
```

Task groups:

- input readiness check
- extract
- validate
- load raw
- transform and load warehouse
- data quality summary

CLI trigger:

```bash
docker exec -it opspulse-airflow-webserver airflow dags trigger opspulse_daily_pipeline --conf "{\"input_path\":\"/opt/airflow/data/raw/workflow_events.csv\"}"
```

## Run the FastAPI Service

Local Python:

```bash
python -m opspulse.api
```

Docker Compose:

```bash
docker compose up -d api
```

## API Endpoints

- `GET /health`
- `GET /api/kpis/summary`
- `GET /api/kpis/daily`
- `GET /api/exceptions`
- `GET /api/exceptions/{workflow_id}`
- `GET /api/backlog`
- `GET /api/workflows/{workflow_id}`
- `GET /api/teams/performance`

Supported filters where appropriate:

- `start_date`
- `end_date`
- `team`
- `priority`
- `status`
- `workflow_type`
- `page`
- `page_size`

## Sample API Calls

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/api/kpis/summary"
curl "http://localhost:8000/api/kpis/daily?page_size=10"
curl "http://localhost:8000/api/exceptions?page_size=10"
curl "http://localhost:8000/api/exceptions/WF-00000001"
curl "http://localhost:8000/api/backlog?team=PaymentsOps"
curl "http://localhost:8000/api/workflows/WF-00000001"
curl "http://localhost:8000/api/teams/performance?page_size=10"
```

## Run Tests

All tests:

```bash
python -m pytest tests
```

Unit and API tests only:

```bash
python -m pytest tests -k "not integration"
```

Integration tests only:

```bash
python -m pytest tests/integration -m integration
```

## Optional Developer Commands

If you use `make`, common shortcuts are available:

```bash
make bootstrap
make generate-data
make etl
make etl-dry
make api
make test
make test-integration
```

## Inspect Warehouse Tables and Views

Quick counts:

```bash
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT COUNT(*) FROM raw.workflow_events_raw;"
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT COUNT(*) FROM staging.workflow_events_quarantine;"
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT COUNT(*) FROM warehouse.fact_workflow_run;"
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT COUNT(*) FROM warehouse.fact_exception;"
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT COUNT(*) FROM marts.kpi_daily_summary;"
```

Reporting views:

```bash
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT * FROM marts.v_team_performance_daily LIMIT 10;"
docker exec -it opspulse-postgres psql -U opspulse -d opspulse -c "SELECT * FROM marts.v_open_exceptions LIMIT 10;"
```

## KPI and Exception Definitions

Important KPIs:

- `total_workflows`: total workflow volume in the KPI slice
- `completed_workflows`: workflows completed in the slice
- `backlog_workflows`: workflows still open or queued
- `exception_workflows`: workflows flagged with operational exceptions
- `sla_breach_count`: workflows that exceeded due time
- `avg_processing_minutes`: average turnaround time in minutes
- `avg_age_hours`: average workflow age in hours
- `throughput_per_assignee`: workflow throughput per assignee
- `data_quality_score`: ETL-derived quality score based on validation and warehouse signals

Current exception categories:

- overdue workflow
- missing assignee
- invalid lifecycle pattern
- high processing time

## Demo Dashboard

A minimal static demo page is available at:

- `docs/demo/index.html`

Serve it:

```bash
cd docs/demo
python -m http.server 9000
```

Then open:

- `http://localhost:9000`

It displays:

- KPI summary
- daily KPI table
- open exceptions

## Limitations

- Tableau is represented by warehouse marts and API/demo outputs, not a committed Tableau workbook
- Airflow metadata and warehouse tables share one PostgreSQL database for local development
- API tests validate HTTP contract and not full warehouse-backed API integration
- Integration tests require a reachable local PostgreSQL instance and configured credentials

## Future Improvements

- add a committed Tableau workbook or screenshots
- add DAG smoke tests and alerting hooks
- add auth and rate limiting
- add richer KPI trend comparisons
- add a more polished dashboard UI
