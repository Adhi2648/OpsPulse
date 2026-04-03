from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from sqlalchemy import text

from opspulse.core.settings import get_settings
from opspulse.db.engine import get_engine
from opspulse.etl.pipeline import (
    ensure_input_exists,
    extract_input,
    load_raw_stage,
    transform_and_load_warehouse,
    validate_input,
)
from opspulse.etl.validate import ValidationResult
from opspulse.utils.logging import configure_logging, get_logger


def _run_directory() -> Path:
    context = get_current_context()
    settings = get_settings()
    logical_date = context["logical_date"].format("YYYYMMDDTHHmmss")
    run_dir = settings.raw_data_dir.parent / "airflow_runs" / logical_date
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _dag_input_path() -> Path:
    context = get_current_context()
    settings = get_settings()
    conf = context["dag_run"].conf or {}
    input_path = Path(conf.get("input_path", settings.raw_data_dir / "workflow_events.csv"))
    return ensure_input_exists(input_path)


default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="opspulse_daily_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["opspulse", "etl", "warehouse"],
)
def opspulse_daily_pipeline() -> None:
    @task(task_id="input_readiness_check")
    def input_readiness_check() -> dict[str, str]:
        settings = get_settings()
        configure_logging(settings.log_level)
        logger = get_logger("opspulse.airflow.input")

        input_path = _dag_input_path()
        run_dir = _run_directory()
        logger.info("Input file ready: %s", input_path)
        logger.info("Airflow run artifacts directory: %s", run_dir)
        return {"input_path": str(input_path), "run_dir": str(run_dir)}

    with TaskGroup(group_id="extract") as extract_group:
        @task(task_id="extract_source")
        def extract_source(run_context: dict[str, str]) -> dict[str, str | int]:
            settings = get_settings()
            configure_logging(settings.log_level)
            logger = get_logger("opspulse.airflow.extract")

            extracted_df = extract_input(Path(run_context["input_path"]))
            extracted_path = Path(run_context["run_dir"]) / "extracted.pkl"
            extracted_df.to_pickle(extracted_path)
            logger.info("Extracted %s rows from %s", len(extracted_df), run_context["input_path"])
            return {
                **run_context,
                "extracted_path": str(extracted_path),
                "extracted_rows": int(len(extracted_df)),
            }

    with TaskGroup(group_id="validate") as validate_group:
        @task(task_id="validate_records")
        def validate_records(extract_context: dict[str, Any]) -> dict[str, Any]:
            settings = get_settings()
            configure_logging(settings.log_level)
            logger = get_logger("opspulse.airflow.validate")

            extracted_df = pd.read_pickle(extract_context["extracted_path"])
            validation_result = validate_input(extracted_df)

            valid_path = Path(extract_context["run_dir"]) / "valid.pkl"
            invalid_path = Path(extract_context["run_dir"]) / "invalid.pkl"
            summary_path = Path(extract_context["run_dir"]) / "validation_summary.json"

            validation_result.valid_df.to_pickle(valid_path)
            validation_result.invalid_df.to_pickle(invalid_path)
            summary_path.write_text(
                json.dumps(validation_result.summary, indent=2),
                encoding="utf-8",
            )

            if validation_result.summary["invalid_rows"] > 0:
                logger.warning(
                    "Validation quarantined %s rows",
                    validation_result.summary["invalid_rows"],
                )
            logger.info("Validation summary: %s", validation_result.summary)
            return {
                **extract_context,
                "valid_path": str(valid_path),
                "invalid_path": str(invalid_path),
                "summary_path": str(summary_path),
            }

    with TaskGroup(group_id="load_raw") as load_raw_group:
        @task(task_id="load_raw_records")
        def load_raw_records_task(validate_context: dict[str, Any]) -> dict[str, Any]:
            settings = get_settings()
            configure_logging(settings.log_level)
            logger = get_logger("opspulse.airflow.load_raw")

            valid_df = pd.read_pickle(validate_context["valid_path"])
            invalid_df = pd.read_pickle(validate_context["invalid_path"])
            summary = json.loads(Path(validate_context["summary_path"]).read_text(encoding="utf-8"))

            validation_result = ValidationResult(
                valid_df=valid_df,
                invalid_df=invalid_df,
                summary=summary,
            )
            raw_key_map = load_raw_stage(validation_result, settings, logger)
            raw_keys_path = Path(validate_context["run_dir"]) / "raw_keys.pkl"
            raw_key_map.to_pickle(raw_keys_path)

            logger.info("Resolved %s raw key mappings", len(raw_key_map))
            return {**validate_context, "raw_keys_path": str(raw_keys_path)}

    with TaskGroup(group_id="transform_and_load_warehouse") as transform_group:
        @task(task_id="transform_and_load")
        def transform_and_load(validate_context: dict[str, Any]) -> dict[str, Any]:
            settings = get_settings()
            configure_logging(settings.log_level)
            logger = get_logger("opspulse.airflow.transform")

            valid_df = pd.read_pickle(validate_context["valid_path"])
            raw_key_map = pd.read_pickle(validate_context["raw_keys_path"])
            staging_with_keys, warehouse_counts = transform_and_load_warehouse(
                valid_df,
                raw_key_map,
                settings,
                logger,
            )
            warehouse_counts_path = Path(validate_context["run_dir"]) / "warehouse_counts.json"
            warehouse_counts_path.write_text(json.dumps(warehouse_counts, indent=2), encoding="utf-8")
            logger.info("Warehouse load counts: %s", warehouse_counts)
            return {
                **validate_context,
                "warehouse_counts_path": str(warehouse_counts_path),
                "staging_rows": int(len(staging_with_keys)),
            }

    with TaskGroup(group_id="data_quality_summary") as quality_group:
        @task(task_id="publish_quality_summary")
        def publish_quality_summary(transform_context: dict[str, Any]) -> dict[str, Any]:
            settings = get_settings()
            configure_logging(settings.log_level)
            logger = get_logger("opspulse.airflow.quality")

            engine = get_engine(settings)
            validation_summary = json.loads(
                Path(transform_context["summary_path"]).read_text(encoding="utf-8")
            )
            warehouse_counts = json.loads(
                Path(transform_context["warehouse_counts_path"]).read_text(encoding="utf-8")
            )

            with engine.begin() as connection:
                db_counts = {
                    "raw.workflow_events_raw": connection.execute(
                        text("SELECT COUNT(*) FROM raw.workflow_events_raw")
                    ).scalar_one(),
                    "staging.workflow_events_clean": connection.execute(
                        text("SELECT COUNT(*) FROM staging.workflow_events_clean")
                    ).scalar_one(),
                    "warehouse.fact_workflow_run": connection.execute(
                        text("SELECT COUNT(*) FROM warehouse.fact_workflow_run")
                    ).scalar_one(),
                    "warehouse.fact_exception": connection.execute(
                        text("SELECT COUNT(*) FROM warehouse.fact_exception")
                    ).scalar_one(),
                    "marts.kpi_daily_summary": connection.execute(
                        text("SELECT COUNT(*) FROM marts.kpi_daily_summary")
                    ).scalar_one(),
                }

            summary = {
                "input_path": transform_context["input_path"],
                "validation_summary": validation_summary,
                "staging_rows": transform_context["staging_rows"],
                "warehouse_load_counts": warehouse_counts,
                "database_row_counts": db_counts,
            }
            logger.info("Data quality summary: %s", summary)
            return summary

    ready = input_readiness_check()
    extracted = extract_source(ready)
    validated = validate_records(extracted)
    raw_loaded = load_raw_records_task(validated)
    transformed = transform_and_load(raw_loaded)
    publish_quality_summary(transformed)


opspulse_daily_pipeline()
