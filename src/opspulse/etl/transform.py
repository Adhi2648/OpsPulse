from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd


TEAM_REFERENCE: dict[str, tuple[str, str]] = {
    "PaymentsOps": ("Finance", "Rina Patel"),
    "CustomerCare": ("Customer Success", "Jordan Lee"),
    "ClaimsReview": ("Operations", "Maya Chen"),
    "RiskOps": ("Operations", "Daniel Kim"),
    "FulfillmentOps": ("Operations", "Elena Garcia"),
}

WORKFLOW_TYPE_REFERENCE: dict[str, tuple[str, int]] = {
    "InvoiceApproval": ("Finance", 24),
    "RefundReview": ("Customer", 12),
    "ClaimsValidation": ("Risk", 36),
    "VendorOnboarding": ("Procurement", 72),
    "KycRefresh": ("Compliance", 48),
}

PRIORITY_RANKS: dict[str, int] = {
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
HIGH_PROCESSING_TIME_HOURS = 24.0


def normalize_workflow_fields(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize status and priority text values used by downstream tables."""
    df = dataframe.copy()
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["priority"] = df["priority"].astype(str).str.strip().str.lower()
    df["workflow_id"] = df["workflow_id"].astype(str).str.strip()
    return df


def add_temporal_metrics(
    dataframe: pd.DataFrame,
    reference_time: datetime | None = None,
) -> pd.DataFrame:
    """Add updated_at, turnaround_hours, age_hours, and processing_minutes columns."""
    df = dataframe.copy()
    resolved_now = reference_time or datetime.now(tz=UTC)
    effective_end = df["completed_at"].fillna(df["started_at"]).fillna(df["created_at"])

    df["updated_at"] = effective_end
    df["turnaround_hours"] = (
        (effective_end - df["created_at"]).dt.total_seconds().fillna(0) / 3600
    ).clip(lower=0)
    df["age_hours"] = (
        (
            df["completed_at"].fillna(pd.Timestamp(resolved_now)) - df["created_at"]
        ).dt.total_seconds().fillna(0)
        / 3600
    ).clip(lower=0)
    df["processing_minutes"] = (df["turnaround_hours"] * 60).round(2)
    return df


def deduplicate_workflows(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest row per workflow_id based on updated_at."""
    df = dataframe.copy()
    if "updated_at" not in df.columns:
        raise ValueError("updated_at column is required for deduplication")

    sorted_df = df.sort_values(["workflow_id", "updated_at"], ascending=[True, False])
    deduped = sorted_df.drop_duplicates(subset=["workflow_id"], keep="first")
    return deduped.reset_index(drop=True)


def apply_sla_breach_flags(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Flag workflows whose due date is earlier than the effective completion state."""
    df = dataframe.copy()
    effective_end = df["completed_at"].fillna(pd.Timestamp.now(tz=UTC))
    df["sla_breached"] = df["due_at"].notna() & (effective_end > df["due_at"])
    return df


def detect_exception_flags(
    dataframe: pd.DataFrame,
    reference_time: datetime | None = None,
) -> pd.DataFrame:
    """Flag exception workflows and classify the dominant exception type."""
    df = dataframe.copy()
    resolved_now = reference_time or datetime.now(tz=UTC)
    open_workflow_mask = ~df["status"].isin(TERMINAL_STATUSES)

    overdue_mask = open_workflow_mask & df["due_at"].notna() & (df["due_at"] < pd.Timestamp(resolved_now))
    missing_assignee_mask = df["assignee_id"].isna() | (df["assignee_id"].astype(str).str.strip() == "")
    invalid_lifecycle_mask = (
        (df["status"] == "completed") & df["completed_at"].isna()
    ) | ((df["started_at"].notna()) & (df["started_at"] < df["created_at"]))
    high_processing_mask = df["turnaround_hours"] > HIGH_PROCESSING_TIME_HOURS

    exception_type = np.select(
        [
            overdue_mask,
            missing_assignee_mask,
            invalid_lifecycle_mask,
            high_processing_mask,
        ],
        [
            "overdue_workflow",
            "missing_assignee",
            "invalid_lifecycle_pattern",
            "high_processing_time",
        ],
        default=df["exception_type"].fillna(""),
    )

    df["exception_type"] = pd.Series(exception_type, index=df.index).replace("", pd.NA)
    df["exception_flag"] = (
        df["exception_flag"].fillna(False)
        | overdue_mask
        | missing_assignee_mask
        | invalid_lifecycle_mask
        | high_processing_mask
    )
    return df


def prepare_staging_dataframe(
    dataframe: pd.DataFrame,
    reference_time: datetime | None = None,
) -> pd.DataFrame:
    """Build the staging dataset used to populate staging and warehouse tables."""
    df = normalize_workflow_fields(dataframe)
    df = add_temporal_metrics(df, reference_time=reference_time)
    df = deduplicate_workflows(df)
    df = apply_sla_breach_flags(df)
    df = detect_exception_flags(df, reference_time=reference_time)
    df["validation_status"] = df.get("validation_status", "valid")
    if "validation_errors" not in df.columns:
        df["validation_errors"] = [[] for _ in range(len(df))]
    return df


def build_dimension_frames(
    staging_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build dimension seed dataframes from staged workflow records."""
    dim_team = (
        staging_df[["team_name"]]
        .drop_duplicates()
        .assign(
            department_name=lambda frame: frame["team_name"].map(
                lambda value: TEAM_REFERENCE.get(value, ("Unknown", "Unknown"))[0]
            ),
            manager_name=lambda frame: frame["team_name"].map(
                lambda value: TEAM_REFERENCE.get(value, ("Unknown", "Unknown"))[1]
            ),
            active_flag=True,
        )
    )

    dim_workflow_type = (
        staging_df[["workflow_type"]]
        .drop_duplicates()
        .assign(
            workflow_domain=lambda frame: frame["workflow_type"].map(
                lambda value: WORKFLOW_TYPE_REFERENCE.get(value, ("Unknown", 24))[0]
            ),
            default_sla_hours=lambda frame: frame["workflow_type"].map(
                lambda value: WORKFLOW_TYPE_REFERENCE.get(value, ("Unknown", 24))[1]
            ),
        )
    )

    dim_priority = pd.DataFrame(
        {"priority_name": list(PRIORITY_RANKS.keys()), "priority_rank": list(PRIORITY_RANKS.values())}
    )
    dim_status = pd.DataFrame(
        {
            "status_name": list(sorted(staging_df["status"].dropna().unique())),
        }
    )
    dim_status["terminal_flag"] = dim_status["status_name"].isin(TERMINAL_STATUSES)

    unique_dates = pd.to_datetime(staging_df["created_at"], utc=True).dt.normalize().dropna().drop_duplicates()
    dim_date = pd.DataFrame({"calendar_date": unique_dates.sort_values()})
    dim_date["date_key"] = dim_date["calendar_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["day_of_week"] = dim_date["calendar_date"].dt.dayofweek + 1
    dim_date["day_name"] = dim_date["calendar_date"].dt.day_name()
    dim_date["week_of_year"] = dim_date["calendar_date"].dt.isocalendar().week.astype(int)
    dim_date["month_number"] = dim_date["calendar_date"].dt.month
    dim_date["month_name"] = dim_date["calendar_date"].dt.month_name()
    dim_date["quarter_number"] = dim_date["calendar_date"].dt.quarter
    dim_date["year_number"] = dim_date["calendar_date"].dt.year

    return {
        "dim_team": dim_team.reset_index(drop=True),
        "dim_workflow_type": dim_workflow_type.reset_index(drop=True),
        "dim_priority": dim_priority.reset_index(drop=True),
        "dim_status": dim_status.reset_index(drop=True),
        "dim_date": dim_date.reset_index(drop=True),
    }


def build_backlog_daily(staging_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily backlog metrics from staged workflow records."""
    df = staging_df.copy()
    df = df.loc[df["backlog_flag"] | ~df["status"].isin(TERMINAL_STATUSES)].copy()
    df["calendar_date"] = pd.to_datetime(df["created_at"], utc=True).dt.normalize()

    backlog_df = (
        df.groupby(["calendar_date", "team_name", "workflow_type", "priority"], dropna=False)
        .agg(
            open_workflow_count=("workflow_id", "count"),
            overdue_workflow_count=("sla_breached", "sum"),
            avg_age_hours=("age_hours", "mean"),
        )
        .reset_index()
    )
    return backlog_df


def build_kpi_summary(staging_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily KPI summaries used by marts.kpi_daily_summary."""
    df = staging_df.copy()
    df["calendar_date"] = pd.to_datetime(df["created_at"], utc=True).dt.normalize()
    df["completed_flag"] = df["status"].eq("completed")
    df["backlog_numeric"] = df["backlog_flag"].astype(int)
    df["exception_numeric"] = df["exception_flag"].astype(int)
    df["sla_breach_numeric"] = df["sla_breached"].astype(int)
    df["assignee_present"] = df["assignee_id"].notna().astype(int)
    df["data_quality_score"] = (
        100
        - (df["error_count"] * 10)
        - ((1 - df["assignee_present"]) * 15)
        - (df["sla_breach_numeric"] * 5)
    ).clip(lower=0, upper=100)

    summary_df = (
        df.groupby(["calendar_date", "team_name", "workflow_type"], dropna=False)
        .agg(
            total_workflows=("workflow_id", "count"),
            completed_workflows=("completed_flag", "sum"),
            backlog_workflows=("backlog_numeric", "sum"),
            exception_workflows=("exception_numeric", "sum"),
            sla_breach_count=("sla_breach_numeric", "sum"),
            avg_processing_minutes=("processing_minutes", "mean"),
            avg_age_hours=("age_hours", "mean"),
            assignee_count=("assignee_id", pd.Series.nunique),
            data_quality_score=("data_quality_score", "mean"),
        )
        .reset_index()
    )

    summary_df["throughput_per_assignee"] = (
        summary_df["total_workflows"] / summary_df["assignee_count"].replace(0, np.nan)
    ).fillna(summary_df["total_workflows"])

    return summary_df.drop(columns=["assignee_count"])
