from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


REQUIRED_COLUMNS = {
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
}

ALLOWED_STATUSES = {"completed", "in_progress", "queued", "failed", "cancelled"}
ALLOWED_PRIORITIES = {"low", "medium", "high", "critical"}
TIMESTAMP_COLUMNS = ("created_at", "started_at", "completed_at", "due_at")


@dataclass(slots=True)
class ValidationResult:
    valid_df: pd.DataFrame
    invalid_df: pd.DataFrame
    summary: dict[str, int]


def missing_required_columns(dataframe: pd.DataFrame) -> set[str]:
    """Return required columns that are absent from the input dataframe."""
    return REQUIRED_COLUMNS.difference(dataframe.columns)


def validate_required_columns(dataframe: pd.DataFrame) -> None:
    """Raise a clear error if the source dataframe does not match the expected schema."""
    missing = missing_required_columns(dataframe)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns: {missing_list}")


def _normalize_text_column(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def validate_workflow_dataframe(dataframe: pd.DataFrame) -> ValidationResult:
    """Validate workflow source data and split it into valid and quarantined records."""
    validate_required_columns(dataframe)

    df = dataframe.copy()
    df["workflow_id"] = _normalize_text_column(df["workflow_id"])
    df["status"] = _normalize_text_column(df["status"]).str.lower()
    df["priority"] = _normalize_text_column(df["priority"]).str.lower()
    df["source_file_name"] = _normalize_text_column(df["source_file_name"])
    df["workflow_type"] = _normalize_text_column(df["workflow_type"])
    df["team_name"] = _normalize_text_column(df["team_name"])
    df["queue_name"] = _normalize_text_column(df["queue_name"])
    df["source_system"] = _normalize_text_column(df["source_system"])
    df["assignee_id"] = _normalize_text_column(df["assignee_id"]).replace("", pd.NA)
    df["exception_type"] = _normalize_text_column(df["exception_type"]).replace("", pd.NA)
    df["backlog_flag"] = df["backlog_flag"].map(_coerce_bool)
    df["exception_flag"] = df["exception_flag"].map(_coerce_bool)
    df["records_touched"] = pd.to_numeric(df["records_touched"], errors="coerce").fillna(0).astype(int)
    df["error_count"] = pd.to_numeric(df["error_count"], errors="coerce").fillna(0).astype(int)
    df["source_row_number"] = pd.to_numeric(
        df["source_row_number"], errors="coerce"
    ).fillna(-1).astype(int)

    df["_validation_errors"] = [[] for _ in range(len(df))]

    for column in TIMESTAMP_COLUMNS:
        original = dataframe[column]
        parsed = pd.to_datetime(original, errors="coerce", utc=True)
        blank_mask = original.isna() | (original.astype(str).str.strip() == "")
        invalid_mask = ~blank_mask & parsed.isna()
        for index in df.index[invalid_mask]:
            df.at[index, "_validation_errors"].append(f"invalid_{column}")
        df[column] = parsed

    empty_workflow_mask = df["workflow_id"] == ""
    for index in df.index[empty_workflow_mask]:
        df.at[index, "_validation_errors"].append("empty_workflow_id")

    invalid_status_mask = ~df["status"].isin(ALLOWED_STATUSES)
    for index in df.index[invalid_status_mask]:
        df.at[index, "_validation_errors"].append("invalid_status")

    invalid_priority_mask = ~df["priority"].isin(ALLOWED_PRIORITIES)
    for index in df.index[invalid_priority_mask]:
        df.at[index, "_validation_errors"].append("invalid_priority")

    completed_at_present = df["completed_at"].notna()
    bad_completed_order = completed_at_present & (df["created_at"] > df["completed_at"])
    for index in df.index[bad_completed_order]:
        df.at[index, "_validation_errors"].append("completed_before_created")

    bad_due_order = df["due_at"].notna() & (df["due_at"] < df["created_at"])
    for index in df.index[bad_due_order]:
        df.at[index, "_validation_errors"].append("due_before_created")

    invalid_mask = df["_validation_errors"].map(bool)

    valid_df = df.loc[~invalid_mask].copy()
    invalid_df = df.loc[invalid_mask].copy()

    valid_df["validation_status"] = "valid"
    valid_df["validation_errors"] = [[] for _ in range(len(valid_df))]

    invalid_df["validation_status"] = "quarantined"
    invalid_df["validation_errors"] = invalid_df["_validation_errors"]

    valid_df = valid_df.drop(columns=["_validation_errors"])
    invalid_df = invalid_df.drop(columns=["_validation_errors"])

    summary = {
        "input_rows": len(df),
        "valid_rows": len(valid_df),
        "invalid_rows": len(invalid_df),
        "quarantined_rows": len(invalid_df),
    }

    return ValidationResult(valid_df=valid_df, invalid_df=invalid_df, summary=summary)
