from __future__ import annotations

import os
import shutil
from pathlib import Path

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
    source_path = ROOT_DIR / "tests" / "fixtures" / "integration_workflow_events.csv"
    csv_path = tmp_path / "integration_fixture.csv"
    shutil.copyfile(source_path, csv_path)
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
