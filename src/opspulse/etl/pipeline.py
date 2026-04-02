from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from opspulse.core.settings import get_settings
from opspulse.db.engine import get_engine
from opspulse.etl.extract import read_workflow_csv
from opspulse.etl.load import (
    load_quarantine_records,
    load_raw_records,
    load_staging_records,
    load_warehouse_tables,
)
from opspulse.etl.transform import prepare_staging_dataframe
from opspulse.etl.validate import ValidationResult, validate_workflow_dataframe
from opspulse.utils.logging import configure_logging, get_logger


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for local ETL execution."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Run the OpsPulse local ETL pipeline.")
    parser.add_argument(
        "--input",
        type=Path,
        default=settings.raw_data_dir / "workflow_events.csv",
        help="Path to the generated workflow CSV input.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and transform data without writing to PostgreSQL.",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Run extract, validate, and transform steps but skip database writes.",
    )
    return parser.parse_args()


def run_pipeline(input_path: Path, dry_run: bool = False, skip_load: bool = False) -> dict[str, object]:
    """Execute the local ETL pipeline from source CSV through warehouse-ready datasets."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("opspulse.etl.pipeline")

    logger.info("Reading workflow source data from %s", input_path)
    extracted_df = read_workflow_csv(input_path)
    logger.info("Extracted %s rows", len(extracted_df))

    validation_result: ValidationResult = validate_workflow_dataframe(extracted_df)
    logger.info("Validation summary: %s", validation_result.summary)

    staging_df = prepare_staging_dataframe(validation_result.valid_df)
    logger.info("Prepared %s deduplicated staging rows", len(staging_df))

    if dry_run or skip_load:
        logger.info("Skipping database writes (dry_run=%s, skip_load=%s)", dry_run, skip_load)
        return {
            "validation_summary": validation_result.summary,
            "staging_rows": len(staging_df),
            "warehouse_load_counts": {},
        }

    engine = get_engine(settings)
    load_quarantine_records(engine, validation_result.invalid_df, settings.diagnostics_dir, logger)
    raw_key_map = load_raw_records(engine, validation_result.valid_df, logger)

    staging_with_keys = staging_df.merge(
        raw_key_map,
        on=["source_file_name", "source_row_number"],
        how="inner",
    )
    if staging_with_keys.empty:
        raise RuntimeError("No raw keys were resolved for staging rows.")

    load_staging_records(engine, staging_with_keys, logger)
    warehouse_counts = load_warehouse_tables(engine, staging_with_keys, logger)

    return {
        "validation_summary": validation_result.summary,
        "staging_rows": len(staging_with_keys),
        "warehouse_load_counts": warehouse_counts,
    }


def main() -> None:
    """CLI entrypoint for python -m opspulse.etl.pipeline."""
    args = parse_args()
    result = run_pipeline(args.input, dry_run=args.dry_run, skip_load=args.skip_load)
    print("OpsPulse ETL completed")
    print(pd.Series(result, dtype="object").to_string())


if __name__ == "__main__":
    main()
