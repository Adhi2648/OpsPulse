from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from opspulse.etl.transform import (
    add_temporal_metrics,
    apply_sla_breach_flags,
    deduplicate_workflows,
    detect_exception_flags,
)


def _base_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "workflow_id": "WF-1",
                "status": "completed",
                "priority": "high",
                "assignee_id": "USR-1",
                "exception_flag": False,
                "exception_type": pd.NA,
                "created_at": pd.Timestamp("2025-01-01T08:00:00Z"),
                "started_at": pd.Timestamp("2025-01-01T08:15:00Z"),
                "completed_at": pd.Timestamp("2025-01-01T12:00:00Z"),
                "due_at": pd.Timestamp("2025-01-01T13:00:00Z"),
            }
        ]
    )


def test_add_temporal_metrics_calculates_turnaround_hours() -> None:
    dataframe = add_temporal_metrics(_base_frame(), reference_time=datetime(2025, 1, 1, 14, 0, tzinfo=UTC))

    assert dataframe.iloc[0]["turnaround_hours"] == 4.0
    assert dataframe.iloc[0]["processing_minutes"] == 240.0
    assert dataframe.iloc[0]["age_hours"] == 4.0


def test_deduplicate_workflows_keeps_latest_updated_at() -> None:
    dataframe = pd.DataFrame(
        [
            {"workflow_id": "WF-1", "updated_at": pd.Timestamp("2025-01-01T09:00:00Z"), "marker": "older"},
            {"workflow_id": "WF-1", "updated_at": pd.Timestamp("2025-01-01T10:00:00Z"), "marker": "latest"},
            {"workflow_id": "WF-2", "updated_at": pd.Timestamp("2025-01-01T08:00:00Z"), "marker": "only"},
        ]
    )

    deduped = deduplicate_workflows(dataframe)

    assert len(deduped) == 2
    assert deduped.loc[deduped["workflow_id"] == "WF-1", "marker"].item() == "latest"


def test_apply_sla_breach_flags_marks_overdue_rows() -> None:
    dataframe = _base_frame()
    dataframe.loc[0, "due_at"] = pd.Timestamp("2025-01-01T11:00:00Z")

    transformed = apply_sla_breach_flags(dataframe)

    assert bool(transformed.iloc[0]["sla_breached"]) is True


def test_detect_exception_flags_identifies_missing_assignee_and_high_processing() -> None:
    dataframe = _base_frame()
    dataframe.loc[0, "assignee_id"] = pd.NA
    dataframe.loc[0, "completed_at"] = pd.Timestamp("2025-01-02T12:30:00Z")
    dataframe = add_temporal_metrics(dataframe, reference_time=datetime(2025, 1, 2, 13, 0, tzinfo=UTC))

    transformed = detect_exception_flags(dataframe, reference_time=datetime(2025, 1, 2, 13, 0, tzinfo=UTC))

    assert bool(transformed.iloc[0]["exception_flag"]) is True
    assert transformed.iloc[0]["exception_type"] == "missing_assignee"
