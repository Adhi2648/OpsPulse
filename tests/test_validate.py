from __future__ import annotations

import pandas as pd
import pytest

from opspulse.etl.validate import validate_required_columns, validate_workflow_dataframe


def _base_row() -> dict[str, object]:
    return {
        "source_file_name": "workflow_events.csv",
        "source_row_number": 1,
        "workflow_id": "WF-00000001",
        "workflow_type": "InvoiceApproval",
        "team_name": "PaymentsOps",
        "assignee_id": "USR-1001",
        "priority": "HIGH",
        "status": "COMPLETED",
        "queue_name": "review",
        "created_at": "2025-01-01T08:00:00Z",
        "started_at": "2025-01-01T08:30:00Z",
        "completed_at": "2025-01-01T10:00:00Z",
        "due_at": "2025-01-02T08:00:00Z",
        "backlog_flag": "false",
        "exception_flag": "false",
        "exception_type": "",
        "source_system": "workday",
        "records_touched": 12,
        "error_count": 0,
        "payload": "{\"records_touched\": 12}",
    }


def test_validate_required_columns_raises_for_missing_columns() -> None:
    dataframe = pd.DataFrame([_base_row()]).drop(columns=["workflow_id"])

    with pytest.raises(ValueError, match="workflow_id"):
        validate_required_columns(dataframe)


def test_validate_workflow_dataframe_splits_valid_and_invalid_rows() -> None:
    valid_row = _base_row()
    invalid_status_row = _base_row() | {
        "source_row_number": 2,
        "workflow_id": "WF-00000002",
        "status": "unknown",
    }
    invalid_dates_row = _base_row() | {
        "source_row_number": 3,
        "workflow_id": "",
        "completed_at": "2024-12-31T12:00:00Z",
        "due_at": "2024-12-30T08:00:00Z",
    }

    result = validate_workflow_dataframe(
        pd.DataFrame([valid_row, invalid_status_row, invalid_dates_row])
    )

    assert result.summary == {
        "input_rows": 3,
        "valid_rows": 1,
        "invalid_rows": 2,
        "quarantined_rows": 2,
    }
    assert result.valid_df.iloc[0]["status"] == "completed"
    assert result.valid_df.iloc[0]["priority"] == "high"

    quarantined_errors = result.invalid_df.set_index("source_row_number")["validation_errors"].to_dict()
    assert "invalid_status" in quarantined_errors[2]
    assert "empty_workflow_id" in quarantined_errors[3]
    assert "completed_before_created" in quarantined_errors[3]
    assert "due_before_created" in quarantined_errors[3]


def test_validate_workflow_dataframe_flags_invalid_timestamps() -> None:
    invalid_timestamp_row = _base_row() | {
        "source_row_number": 4,
        "workflow_id": "WF-00000004",
        "started_at": "not-a-timestamp",
    }

    result = validate_workflow_dataframe(pd.DataFrame([invalid_timestamp_row]))

    assert result.valid_df.empty
    assert result.invalid_df.iloc[0]["validation_status"] == "quarantined"
    assert "invalid_started_at" in result.invalid_df.iloc[0]["validation_errors"]
