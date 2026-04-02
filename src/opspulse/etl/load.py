from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from opspulse.etl.transform import build_backlog_daily, build_dimension_frames, build_kpi_summary


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    """Check whether a table exists in the connected PostgreSQL database."""
    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_name = :table_name
        )
        """
    )
    with engine.begin() as connection:
        return bool(
            connection.execute(query, {"schema_name": schema, "table_name": table}).scalar_one()
        )


def _dataframe_to_records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    return dataframe.where(pd.notnull(dataframe), None).to_dict(orient="records")


def _chunked(records: list[dict[str, Any]], size: int = 5000) -> list[list[dict[str, Any]]]:
    return [records[index : index + size] for index in range(0, len(records), size)]


def load_quarantine_records(
    engine: Engine,
    invalid_df: pd.DataFrame,
    diagnostics_dir: Path,
    logger: Any,
) -> None:
    """Persist quarantined rows to PostgreSQL when possible, otherwise to a CSV file."""
    if invalid_df.empty:
        return

    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    if not table_exists(engine, "staging", "workflow_events_quarantine"):
        output_path = diagnostics_dir / "workflow_events_quarantine.csv"
        invalid_df.to_csv(output_path, index=False)
        logger.warning("Quarantine table missing. Saved invalid rows to %s", output_path)
        return

    quarantine_df = invalid_df.copy()
    quarantine_df["raw_payload"] = quarantine_df.apply(
        lambda row: json.dumps(row.drop(labels=["validation_errors"]).to_dict(), default=str),
        axis=1,
    )

    records = _dataframe_to_records(
        quarantine_df[
            [
                "source_file_name",
                "source_row_number",
                "workflow_id",
                "validation_errors",
                "raw_payload",
            ]
        ]
    )

    statement = text(
        """
        INSERT INTO staging.workflow_events_quarantine (
            source_file_name,
            source_row_number,
            workflow_id,
            validation_errors,
            raw_payload
        ) VALUES (
            :source_file_name,
            :source_row_number,
            :workflow_id,
            :validation_errors,
            CAST(:raw_payload AS JSONB)
        )
        ON CONFLICT (source_file_name, source_row_number)
        DO UPDATE SET
            workflow_id = EXCLUDED.workflow_id,
            validation_errors = EXCLUDED.validation_errors,
            raw_payload = EXCLUDED.raw_payload,
            quarantined_at = NOW()
        """
    )

    with engine.begin() as connection:
        for chunk in _chunked(records):
            connection.execute(statement, chunk)

    logger.info("Loaded %s quarantined rows", len(records))


def load_raw_records(engine: Engine, valid_df: pd.DataFrame, logger: Any) -> pd.DataFrame:
    """Bulk insert valid records into raw.workflow_events_raw and return raw key mappings."""
    records = _dataframe_to_records(
        valid_df[
            [
                "source_file_name",
                "source_row_number",
                "workflow_id",
                "workflow_type",
                "team_name",
                "assignee_id",
                "priority",
                "status",
                "queue_name",
                "created_at",
                "started_at",
                "completed_at",
                "due_at",
                "backlog_flag",
                "exception_flag",
                "exception_type",
                "source_system",
                "records_touched",
                "error_count",
                "payload",
            ]
        ]
    )

    statement = text(
        """
        INSERT INTO raw.workflow_events_raw (
            source_file_name,
            source_row_number,
            workflow_id,
            workflow_type,
            team_name,
            assignee_id,
            priority,
            status,
            queue_name,
            created_at,
            started_at,
            completed_at,
            due_at,
            backlog_flag,
            exception_flag,
            exception_type,
            source_system,
            records_touched,
            error_count,
            payload
        ) VALUES (
            :source_file_name,
            :source_row_number,
            :workflow_id,
            :workflow_type,
            :team_name,
            :assignee_id,
            :priority,
            :status,
            :queue_name,
            :created_at,
            :started_at,
            :completed_at,
            :due_at,
            :backlog_flag,
            :exception_flag,
            :exception_type,
            :source_system,
            :records_touched,
            :error_count,
            CAST(:payload AS JSONB)
        )
        ON CONFLICT (source_file_name, source_row_number) DO NOTHING
        """
    )

    with engine.begin() as connection:
        for chunk in _chunked(records):
            connection.execute(statement, chunk)

        source_files = tuple(valid_df["source_file_name"].dropna().unique().tolist())
        mapping_result = connection.execute(
            text(
                """
                SELECT raw_event_id, source_file_name, source_row_number
                FROM raw.workflow_events_raw
                WHERE source_file_name = ANY(:source_files)
                """
            ),
            {"source_files": list(source_files)},
        )
        mapping_df = pd.DataFrame(mapping_result.fetchall(), columns=mapping_result.keys())

    logger.info("Loaded %s valid raw rows", len(valid_df))
    return mapping_df


def load_staging_records(engine: Engine, staging_df: pd.DataFrame, logger: Any) -> None:
    """Refresh staging rows for the current raw_event_ids and insert transformed data."""
    if staging_df.empty:
        logger.warning("No staging rows to load")
        return

    records = _dataframe_to_records(
        staging_df[
            [
                "raw_event_id",
                "workflow_id",
                "workflow_type",
                "team_name",
                "assignee_id",
                "priority",
                "status",
                "queue_name",
                "created_at",
                "started_at",
                "completed_at",
                "due_at",
                "processing_minutes",
                "age_hours",
                "sla_breached",
                "backlog_flag",
                "exception_flag",
                "exception_type",
                "records_touched",
                "error_count",
                "validation_status",
                "validation_errors",
            ]
        ]
    )
    raw_event_ids = staging_df["raw_event_id"].astype(int).tolist()

    delete_statement = text(
        "DELETE FROM staging.workflow_events_clean WHERE raw_event_id = ANY(:raw_event_ids)"
    )
    insert_statement = text(
        """
        INSERT INTO staging.workflow_events_clean (
            raw_event_id,
            workflow_id,
            workflow_type,
            team_name,
            assignee_id,
            priority,
            status,
            queue_name,
            created_at,
            started_at,
            completed_at,
            due_at,
            processing_minutes,
            age_hours,
            sla_breached,
            backlog_flag,
            exception_flag,
            exception_type,
            records_touched,
            error_count,
            validation_status,
            validation_errors
        ) VALUES (
            :raw_event_id,
            :workflow_id,
            :workflow_type,
            :team_name,
            :assignee_id,
            :priority,
            :status,
            :queue_name,
            :created_at,
            :started_at,
            :completed_at,
            :due_at,
            :processing_minutes,
            :age_hours,
            :sla_breached,
            :backlog_flag,
            :exception_flag,
            :exception_type,
            :records_touched,
            :error_count,
            :validation_status,
            :validation_errors
        )
        """
    )

    with engine.begin() as connection:
        connection.execute(delete_statement, {"raw_event_ids": raw_event_ids})
        for chunk in _chunked(records):
            connection.execute(insert_statement, chunk)

    logger.info("Loaded %s staging rows", len(staging_df))


def _upsert_dimension_table(
    engine: Engine,
    dataframe: pd.DataFrame,
    table_name: str,
    key_columns: list[str],
    update_columns: list[str],
) -> None:
    if dataframe.empty:
        return

    columns = key_columns + update_columns
    insert_columns = ", ".join(columns)
    values_clause = ", ".join(f":{column}" for column in columns)
    conflict_clause = ", ".join(key_columns)
    update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)

    statement = text(
        f"""
        INSERT INTO {table_name} ({insert_columns})
        VALUES ({values_clause})
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_clause}
        """
    )

    records = _dataframe_to_records(dataframe[columns])
    with engine.begin() as connection:
        for chunk in _chunked(records):
            connection.execute(statement, chunk)


def load_dimension_tables(engine: Engine, staging_df: pd.DataFrame, logger: Any) -> None:
    """Upsert warehouse dimension tables derived from staged workflow data."""
    dimensions = build_dimension_frames(staging_df)
    _upsert_dimension_table(
        engine,
        dimensions["dim_team"],
        "warehouse.dim_team",
        ["team_name"],
        ["department_name", "manager_name", "active_flag"],
    )
    _upsert_dimension_table(
        engine,
        dimensions["dim_workflow_type"],
        "warehouse.dim_workflow_type",
        ["workflow_type"],
        ["workflow_domain", "default_sla_hours"],
    )
    _upsert_dimension_table(
        engine,
        dimensions["dim_priority"],
        "warehouse.dim_priority",
        ["priority_name"],
        ["priority_rank"],
    )
    _upsert_dimension_table(
        engine,
        dimensions["dim_status"],
        "warehouse.dim_status",
        ["status_name"],
        ["terminal_flag"],
    )
    _upsert_dimension_table(
        engine,
        dimensions["dim_date"],
        "warehouse.dim_date",
        ["date_key"],
        [
            "calendar_date",
            "day_of_week",
            "day_name",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter_number",
            "year_number",
        ],
    )
    logger.info("Upserted warehouse dimensions")


def _fetch_dimension_map(engine: Engine, query: str, key_column: str, value_column: str) -> dict[Any, Any]:
    with engine.begin() as connection:
        result = connection.execute(text(query))
        frame = pd.DataFrame(result.fetchall(), columns=result.keys())
    return dict(zip(frame[key_column], frame[value_column], strict=False))


def _build_fact_frames(engine: Engine, staging_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    team_map = _fetch_dimension_map(
        engine,
        "SELECT team_key, team_name FROM warehouse.dim_team",
        "team_name",
        "team_key",
    )
    workflow_type_map = _fetch_dimension_map(
        engine,
        "SELECT workflow_type_key, workflow_type FROM warehouse.dim_workflow_type",
        "workflow_type",
        "workflow_type_key",
    )
    priority_map = _fetch_dimension_map(
        engine,
        "SELECT priority_key, priority_name FROM warehouse.dim_priority",
        "priority_name",
        "priority_key",
    )
    status_map = _fetch_dimension_map(
        engine,
        "SELECT status_key, status_name FROM warehouse.dim_status",
        "status_name",
        "status_key",
    )
    date_map = _fetch_dimension_map(
        engine,
        "SELECT date_key, calendar_date FROM warehouse.dim_date",
        "calendar_date",
        "date_key",
    )

    fact_df = staging_df.copy()
    fact_df["calendar_date"] = pd.to_datetime(fact_df["created_at"], utc=True).dt.normalize().dt.date
    fact_df["date_key"] = fact_df["calendar_date"].map(date_map)
    fact_df["team_key"] = fact_df["team_name"].map(team_map)
    fact_df["workflow_type_key"] = fact_df["workflow_type"].map(workflow_type_map)
    fact_df["priority_key"] = fact_df["priority"].map(priority_map)
    fact_df["status_key"] = fact_df["status"].map(status_map)

    fact_workflow_run = fact_df[
        [
            "workflow_id",
            "date_key",
            "team_key",
            "workflow_type_key",
            "priority_key",
            "status_key",
            "queue_name",
            "source_system",
            "created_at",
            "started_at",
            "completed_at",
            "due_at",
            "processing_minutes",
            "age_hours",
            "records_touched",
            "error_count",
            "backlog_flag",
            "exception_flag",
            "sla_breached",
        ]
    ].copy()

    exception_df = fact_df.loc[fact_df["exception_flag"]].copy()
    fact_exception = exception_df[
        [
            "workflow_id",
            "date_key",
            "team_key",
            "workflow_type_key",
            "priority_key",
            "exception_type",
            "status",
            "error_count",
            "created_at",
        ]
    ].rename(
        columns={
            "status": "status_name",
            "created_at": "detected_at",
        }
    )
    fact_exception["resolved_at"] = None
    fact_exception["open_flag"] = ~fact_df.loc[fact_df["exception_flag"], "status"].isin(
        ["completed", "cancelled"]
    )

    backlog_df = build_backlog_daily(staging_df)
    backlog_df["date_key"] = pd.to_datetime(backlog_df["calendar_date"], utc=True).dt.date.map(date_map)
    backlog_df["team_key"] = backlog_df["team_name"].map(team_map)
    backlog_df["workflow_type_key"] = backlog_df["workflow_type"].map(workflow_type_map)
    backlog_df["priority_key"] = backlog_df["priority"].map(priority_map)
    fact_backlog_daily = backlog_df[
        [
            "date_key",
            "team_key",
            "workflow_type_key",
            "priority_key",
            "open_workflow_count",
            "overdue_workflow_count",
            "avg_age_hours",
        ]
    ].copy()

    kpi_df = build_kpi_summary(staging_df)
    kpi_df["date_key"] = pd.to_datetime(kpi_df["calendar_date"], utc=True).dt.date.map(date_map)
    kpi_df["team_key"] = kpi_df["team_name"].map(team_map)
    kpi_df["workflow_type_key"] = kpi_df["workflow_type"].map(workflow_type_map)
    kpi_summary = kpi_df[
        [
            "date_key",
            "team_key",
            "workflow_type_key",
            "total_workflows",
            "completed_workflows",
            "backlog_workflows",
            "exception_workflows",
            "sla_breach_count",
            "avg_processing_minutes",
            "avg_age_hours",
            "throughput_per_assignee",
            "data_quality_score",
        ]
    ].copy()

    return fact_workflow_run, fact_exception, fact_backlog_daily, kpi_summary


def _delete_existing_slices(connection: Any, date_keys: list[int]) -> None:
    if not date_keys:
        return
    tables = [
        "warehouse.fact_workflow_run",
        "warehouse.fact_exception",
        "warehouse.fact_backlog_daily",
        "marts.kpi_daily_summary",
    ]
    for table_name in tables:
        connection.execute(text(f"DELETE FROM {table_name} WHERE date_key = ANY(:date_keys)"), {"date_keys": date_keys})


def _insert_table(connection: Any, table_name: str, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        return
    records = _dataframe_to_records(dataframe)
    columns = list(dataframe.columns)
    statement = text(
        f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({", ".join(f":{column}" for column in columns)})
        """
    )
    for chunk in _chunked(records):
        connection.execute(statement, chunk)


def load_warehouse_tables(engine: Engine, staging_df: pd.DataFrame, logger: Any) -> dict[str, int]:
    """Populate dimensions, facts, and marts from staged workflow records."""
    load_dimension_tables(engine, staging_df, logger)
    fact_workflow_run, fact_exception, fact_backlog_daily, kpi_summary = _build_fact_frames(
        engine, staging_df
    )
    date_keys = sorted(
        {
            int(value)
            for value in fact_workflow_run["date_key"].dropna().astype(int).tolist()
        }
    )

    with engine.begin() as connection:
        _delete_existing_slices(connection, date_keys)
        _insert_table(connection, "warehouse.fact_workflow_run", fact_workflow_run)
        _insert_table(connection, "warehouse.fact_exception", fact_exception)
        _insert_table(connection, "warehouse.fact_backlog_daily", fact_backlog_daily)
        _insert_table(connection, "marts.kpi_daily_summary", kpi_summary)

    counts = {
        "fact_workflow_run": len(fact_workflow_run),
        "fact_exception": len(fact_exception),
        "fact_backlog_daily": len(fact_backlog_daily),
        "kpi_daily_summary": len(kpi_summary),
    }
    logger.info("Loaded warehouse tables: %s", counts)
    return counts
