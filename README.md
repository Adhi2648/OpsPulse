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

### 2. Install the package for local module execution

```bash
python -m pip install -e .
```

### 3. Start PostgreSQL and Airflow

```bash
docker compose up -d postgres airflow-init airflow-scheduler airflow-webserver
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

## Warehouse Layout

- `raw`: immutable landing tables for source workflow events
- `staging`: validated and standardized workflow records
- `warehouse`: fact and dimension tables for analytics
- `marts`: KPI summaries and reporting views

See `docs/architecture.md` for the system design and `sql/init/` for the database schema.
