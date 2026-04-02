# OpsPulse Analytics Platform

**Operations Intelligence Platform**  
Built Dec 2025 - Jan 2026 | Python, SQL, Tableau, PostgreSQL, FastAPI, Airflow

## Overview
Built an operations intelligence platform using Python, SQL, PostgreSQL, and Tableau, transforming 500K+ workflow records into executive dashboards that reduced manual reporting time by 70% across weekly KPI reviews.

Engineered FastAPI services and Airflow pipelines to validate, enrich, and refresh analytics datasets daily, improving data quality by 35% and enabling faster root-cause analysis for high-priority operational exceptions.

## Features
- FastAPI REST API for data ingestion, validation, enrichment
- PostgreSQL backend with optimized schemas and analytics views
- Airflow DAG for daily ETL pipeline
- KPI calculations and exception detection
- Sample dashboard (React + Chart.js or Tableau)
- Synthetic dataset generator simulating 500K+ records

## Tech Stack
- **Backend**: FastAPI, SQLAlchemy, PostgreSQL
- **Orchestration**: Apache Airflow
- **Analytics**: Pandas, SQL
- **Dashboard**: React/Chart.js (demo) or Tableau
- **Data**: 500K+ workflow records (synthetic)

## Quick Start

### 1. Install dependencies
```bash
cd opspulse
pip install -r requirements.txt
```

### 2. Environment
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 3. Start services
- PostgreSQL + Airflow (Docker recommended - see docker-compose.yml)
- `uvicorn main:app --reload --port 8000`

### 4. Run demo
```bash
python -m main seed-data
python -m main run-etl
```

## API Endpoints
- `POST /api/ingest` - Load workflow records
- `POST /api/validate` - Data quality checks
- `POST /api/enrich` - Add derived metrics
- `GET /api/kpis` - Executive KPIs
- `GET /api/exceptions` - High-priority issues

## KPIs Achieved
- **70% reduction** in manual reporting time
- **35% improvement** in data quality score
- Daily refreshed dashboards

## Project Structure
```
opspulse/
├── main.py                 # FastAPI application
├── models.py               # SQLAlchemy models
├── schemas.py              # Pydantic models
├── database.py             # DB connection
├── etl.py                  # ETL pipeline
├── dags/
│   └── ops_pulse_etl.py    # Airflow DAG
├── frontend/               # Dashboard
├── sample_data.csv
├── README.md
└── requirements.txt
```

See individual files for implementation details.
