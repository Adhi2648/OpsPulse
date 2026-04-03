from __future__ import annotations

import pytest
from sqlalchemy import text

from opspulse.etl.pipeline import run_pipeline


@pytest.mark.integration
def test_pipeline_loads_postgres_tables(
    integration_engine,
    integration_csv,
    integration_env,
) -> None:
    result = run_pipeline(integration_csv)

    assert result["validation_summary"]["valid_rows"] == 2
    assert result["validation_summary"]["invalid_rows"] == 1
    assert result["staging_rows"] == 2

    with integration_engine.begin() as connection:
        raw_count = connection.execute(text("SELECT COUNT(*) FROM raw.workflow_events_raw")).scalar_one()
        quarantine_count = connection.execute(
            text("SELECT COUNT(*) FROM staging.workflow_events_quarantine")
        ).scalar_one()
        fact_count = connection.execute(
            text("SELECT COUNT(*) FROM warehouse.fact_workflow_run")
        ).scalar_one()
        exception_count = connection.execute(
            text("SELECT COUNT(*) FROM warehouse.fact_exception")
        ).scalar_one()
        kpi_count = connection.execute(
            text("SELECT COUNT(*) FROM marts.kpi_daily_summary")
        ).scalar_one()

    assert raw_count == 2
    assert quarantine_count == 1
    assert fact_count == 2
    assert exception_count >= 1
    assert kpi_count >= 1


@pytest.mark.integration
def test_pipeline_rerun_is_basic_idempotent(
    integration_engine,
    integration_csv,
    integration_env,
) -> None:
    first_result = run_pipeline(integration_csv)
    second_result = run_pipeline(integration_csv)

    assert first_result["validation_summary"] == second_result["validation_summary"]

    with integration_engine.begin() as connection:
        raw_count = connection.execute(text("SELECT COUNT(*) FROM raw.workflow_events_raw")).scalar_one()
        quarantine_count = connection.execute(
            text("SELECT COUNT(*) FROM staging.workflow_events_quarantine")
        ).scalar_one()
        fact_count = connection.execute(
            text("SELECT COUNT(*) FROM warehouse.fact_workflow_run")
        ).scalar_one()
        kpi_count = connection.execute(
            text("SELECT COUNT(*) FROM marts.kpi_daily_summary")
        ).scalar_one()

    assert raw_count == 2
    assert quarantine_count == 1
    assert fact_count == 2
    assert kpi_count >= 1
