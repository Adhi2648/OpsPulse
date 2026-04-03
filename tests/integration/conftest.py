from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from opspulse.core.settings import get_settings


ROOT_DIR = Path(__file__).resolve().parents[2]
SQL_INIT_DIR = ROOT_DIR / "sql" / "init"


def _database_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "opspulse")
    user = os.getenv("POSTGRES_USER", "opspulse")
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        pytest.skip("Set POSTGRES_PASSWORD and start local PostgreSQL to run integration tests.")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="session")
def integration_engine():
    engine = create_engine(_database_url(), future=True, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:  # pragma: no cover
        pytest.skip(f"PostgreSQL is not reachable for integration tests: {exc}")

    raw_connection = engine.raw_connection()
    try:
        cursor = raw_connection.cursor()
        for sql_file in sorted(SQL_INIT_DIR.glob("*.sql")):
            cursor.execute(sql_file.read_text(encoding="utf-8"))
        raw_connection.commit()
    finally:
        raw_connection.close()

    return engine


@pytest.fixture(autouse=True)
def clean_warehouse_tables(integration_engine):
    truncate_sql = """
    TRUNCATE TABLE
        marts.kpi_daily_summary,
        warehouse.fact_backlog_daily,
        warehouse.fact_exception,
        warehouse.fact_workflow_run,
        staging.workflow_events_clean,
        staging.workflow_events_quarantine,
        raw.workflow_events_raw,
        warehouse.dim_date,
        warehouse.dim_status,
        warehouse.dim_priority,
        warehouse.dim_workflow_type,
        warehouse.dim_team
    RESTART IDENTITY CASCADE
    """
    with integration_engine.begin() as connection:
        connection.execute(text(truncate_sql))
    yield


@pytest.fixture
def integration_csv(tmp_path: Path) -> Path:
    dataframe = pd.DataFrame(
        [
            {
                "source_file_name": "integration_fixture.csv",
                "source_row_number": 1,
                "workflow_id": "WF-INT-001",
                "workflow_type": "InvoiceApproval",
                "team_name": "PaymentsOps",
                "assignee_id": "USR-2001",
                "priority": "high",
                "status": "completed",
                "queue_name": "review",
                "created_at": "2025-01-01T08:00:00Z",
                "started_at": "2025-01-01T08:15:00Z",
                "completed_at": "2025-01-01T10:00:00Z",
                "due_at": "2025-01-01T12:00:00Z",
                "backlog_flag": "false",
                "exception_flag": "false",
                "exception_type": "",
                "source_system": "workday",
                "records_touched": 10,
                "error_count": 0,
                "payload": "{\"records_touched\": 10}",
            },
            {
                "source_file_name": "integration_fixture.csv",
                "source_row_number": 2,
                "workflow_id": "WF-INT-002",
                "workflow_type": "RefundReview",
                "team_name": "CustomerCare",
                "assignee_id": "",
                "priority": "critical",
                "status": "queued",
                "queue_name": "exceptions",
                "created_at": "2025-01-01T09:00:00Z",
                "started_at": "2025-01-01T09:10:00Z",
                "completed_at": "",
                "due_at": "2025-01-01T10:00:00Z",
                "backlog_flag": "true",
                "exception_flag": "false",
                "exception_type": "",
                "source_system": "zendesk",
                "records_touched": 5,
                "error_count": 1,
                "payload": "{\"records_touched\": 5}",
            },
            {
                "source_file_name": "integration_fixture.csv",
                "source_row_number": 3,
                "workflow_id": "",
                "workflow_type": "ClaimsValidation",
                "team_name": "ClaimsReview",
                "assignee_id": "USR-2003",
                "priority": "medium",
                "status": "completed",
                "queue_name": "approval",
                "created_at": "2025-01-01T11:00:00Z",
                "started_at": "2025-01-01T11:10:00Z",
                "completed_at": "2024-12-31T11:00:00Z",
                "due_at": "2025-01-01T15:00:00Z",
                "backlog_flag": "false",
                "exception_flag": "false",
                "exception_type": "",
                "source_system": "salesforce",
                "records_touched": 12,
                "error_count": 0,
                "payload": "{\"records_touched\": 12}",
            },
        ]
    )
    csv_path = tmp_path / "integration_fixture.csv"
    dataframe.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def integration_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POSTGRES_HOST", os.getenv("POSTGRES_HOST", "localhost"))
    monkeypatch.setenv("POSTGRES_PORT", os.getenv("POSTGRES_PORT", "5432"))
    monkeypatch.setenv("POSTGRES_DB", os.getenv("POSTGRES_DB", "opspulse"))
    monkeypatch.setenv("POSTGRES_USER", os.getenv("POSTGRES_USER", "opspulse"))
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        pytest.skip("Set POSTGRES_PASSWORD and start local PostgreSQL to run integration tests.")
    monkeypatch.setenv("POSTGRES_PASSWORD", password)
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
