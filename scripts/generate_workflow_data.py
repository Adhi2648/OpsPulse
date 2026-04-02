from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path


TEAMS = (
    ("PaymentsOps", "Finance", "Rina Patel"),
    ("CustomerCare", "Customer Success", "Jordan Lee"),
    ("ClaimsReview", "Operations", "Maya Chen"),
    ("RiskOps", "Operations", "Daniel Kim"),
    ("FulfillmentOps", "Operations", "Elena Garcia"),
)

WORKFLOW_TYPES = (
    ("InvoiceApproval", "Finance", 24),
    ("RefundReview", "Customer", 12),
    ("ClaimsValidation", "Risk", 36),
    ("VendorOnboarding", "Procurement", 72),
    ("KycRefresh", "Compliance", 48),
)

PRIORITIES = ("low", "medium", "high", "critical")
STATUSES = ("completed", "in_progress", "queued", "failed", "cancelled")
QUEUES = ("intake", "review", "approval", "exceptions", "fulfillment")
SOURCE_SYSTEMS = ("salesforce", "zendesk", "netsuite", "workday", "internal_ops")
EXCEPTION_TYPES = (
    "missing_metadata",
    "sla_breach",
    "duplicate_submission",
    "validation_error",
    "stuck_in_queue",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic OpsPulse workflow source data."
    )
    parser.add_argument(
        "--records",
        type=int,
        default=500_000,
        help="Number of workflow records to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible generation.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory where generated CSV and metadata files will be written.",
    )
    return parser.parse_args()


def weighted_status() -> str:
    return random.choices(
        population=STATUSES,
        weights=(0.61, 0.16, 0.14, 0.06, 0.03),
        k=1,
    )[0]


def weighted_priority() -> str:
    return random.choices(
        population=PRIORITIES,
        weights=(0.28, 0.44, 0.22, 0.06),
        k=1,
    )[0]


def build_payload(records_touched: int, error_count: int, status: str) -> dict[str, object]:
    return {
        "channel": random.choice(["api", "manual", "batch", "partner"]),
        "records_touched": records_touched,
        "error_count": error_count,
        "status_reason": "auto_completed" if status == "completed" else "manual_review",
        "retry_count": max(0, error_count - 1),
    }


def build_row(index: int, base_time: datetime) -> dict[str, object]:
    team_name, _, _ = random.choice(TEAMS)
    workflow_type, _, sla_hours = random.choice(WORKFLOW_TYPES)
    created_at = base_time + timedelta(minutes=index % 1440, days=index // 12000)
    status = weighted_status()
    priority = weighted_priority()
    records_touched = random.randint(1, 250)
    error_count = random.choices([0, 1, 2, 3, 4], weights=(0.73, 0.17, 0.06, 0.03, 0.01), k=1)[0]
    backlog_flag = status in {"queued", "in_progress"}
    exception_flag = error_count >= 2 or status == "failed"

    started_at = created_at + timedelta(minutes=random.randint(2, 60))
    processing_minutes = random.randint(5, sla_hours * 60)
    completed_at = None
    if status in {"completed", "failed", "cancelled"}:
        completed_at = started_at + timedelta(minutes=processing_minutes)

    due_at = created_at + timedelta(hours=sla_hours)
    exception_type = random.choice(EXCEPTION_TYPES) if exception_flag else ""

    return {
        "source_file_name": "workflow_events.csv",
        "source_row_number": index + 1,
        "workflow_id": f"WF-{index + 1:08d}",
        "workflow_type": workflow_type,
        "team_name": team_name,
        "assignee_id": f"USR-{random.randint(1000, 1499)}",
        "priority": priority,
        "status": status,
        "queue_name": random.choice(QUEUES),
        "created_at": created_at.isoformat(),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat() if completed_at else "",
        "due_at": due_at.isoformat(),
        "backlog_flag": str(backlog_flag).lower(),
        "exception_flag": str(exception_flag).lower(),
        "exception_type": exception_type,
        "source_system": random.choice(SOURCE_SYSTEMS),
        "records_touched": records_touched,
        "error_count": error_count,
        "payload": json.dumps(build_payload(records_touched, error_count, status)),
    }


def generate_dataset(record_count: int, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "workflow_events.csv"
    metadata_path = output_dir / "workflow_events_metadata.json"

    base_time = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    fieldnames = [
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

    exception_rows = 0
    backlog_rows = 0

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for index in range(record_count):
            row = build_row(index=index, base_time=base_time)
            writer.writerow(row)
            if row["exception_flag"] == "true":
                exception_rows += 1
            if row["backlog_flag"] == "true":
                backlog_rows += 1

    metadata = {
        "record_count": record_count,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "csv_path": str(csv_path),
        "exception_rows": exception_rows,
        "backlog_rows": backlog_rows,
        "teams": [team[0] for team in TEAMS],
        "workflow_types": [workflow_type[0] for workflow_type in WORKFLOW_TYPES],
    }

    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return csv_path, metadata_path


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    csv_path, metadata_path = generate_dataset(
        record_count=args.records,
        output_dir=args.output_dir,
    )
    print(f"Generated {args.records} workflow records at {csv_path}")
    print(f"Metadata written to {metadata_path}")


if __name__ == "__main__":
    main()
