from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from opspulse.api.dependencies import get_db
from opspulse.api.main import create_app


class DummySession:
    pass


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_db] = lambda: iter([DummySession()])

    monkeypatch.setattr(
        "opspulse.api.routes.check_database_health",
        lambda: True,
    )
    return TestClient(app)


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"


def test_kpi_summary_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_kpi_summary",
        lambda *args, **kwargs: {
            "total_workflows": 100,
            "completed_workflows": 80,
            "backlog_workflows": 15,
            "exception_workflows": 5,
            "sla_breach_count": 4,
            "avg_processing_minutes": 32.5,
            "avg_age_hours": 4.2,
            "throughput_per_assignee": 12.0,
            "data_quality_score": 94.7,
        },
    )

    response = client.get("/api/kpis/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["total_workflows"] == 100
    assert "filters" in body


def test_kpi_daily_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_kpi_daily",
        lambda *args, **kwargs: (
            [
                {
                    "calendar_date": "2025-01-01",
                    "team_name": "PaymentsOps",
                    "workflow_type": "InvoiceApproval",
                    "total_workflows": 100,
                    "completed_workflows": 85,
                    "backlog_workflows": 10,
                    "exception_workflows": 5,
                    "sla_breach_count": 4,
                    "avg_processing_minutes": 31.2,
                    "avg_age_hours": 4.8,
                    "throughput_per_assignee": 9.5,
                    "data_quality_score": 95.1,
                }
            ],
            1,
        ),
    )

    response = client.get("/api/kpis/daily")

    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 1
    assert body["pagination"]["total"] == 1


def test_exceptions_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_exceptions",
        lambda *args, **kwargs: (
            [
                {
                    "workflow_id": "WF-001",
                    "detected_date": "2025-01-01",
                    "team_name": "CustomerCare",
                    "workflow_type": "RefundReview",
                    "priority_name": "critical",
                    "exception_type": "missing_assignee",
                    "status_name": "queued",
                    "error_count": 1,
                    "open_flag": True,
                }
            ],
            1,
        ),
    )

    response = client.get("/api/exceptions")

    assert response.status_code == 200
    assert response.json()["items"][0]["workflow_id"] == "WF-001"


def test_exception_detail_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_exceptions",
        lambda *args, **kwargs: (
            [
                {
                    "workflow_id": "WF-001",
                    "detected_date": "2025-01-01",
                    "team_name": "CustomerCare",
                    "workflow_type": "RefundReview",
                    "priority_name": "critical",
                    "exception_type": "missing_assignee",
                    "status_name": "queued",
                    "error_count": 1,
                    "open_flag": True,
                }
            ],
            1,
        ),
    )

    response = client.get("/api/exceptions/WF-001")

    assert response.status_code == 200
    assert response.json()["workflow_id"] == "WF-001"
    assert len(response.json()["exceptions"]) == 1


def test_workflow_detail_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_workflow_detail",
        lambda *args, **kwargs: {
            "workflow_id": "WF-001",
            "calendar_date": "2025-01-01",
            "team_name": "PaymentsOps",
            "workflow_type": "InvoiceApproval",
            "priority_name": "high",
            "status_name": "completed",
            "queue_name": "review",
            "source_system": "workday",
            "created_at": datetime(2025, 1, 1, 8, 0, tzinfo=UTC),
            "started_at": datetime(2025, 1, 1, 8, 15, tzinfo=UTC),
            "completed_at": datetime(2025, 1, 1, 9, 30, tzinfo=UTC),
            "due_at": datetime(2025, 1, 1, 12, 0, tzinfo=UTC),
            "processing_minutes": 90.0,
            "age_hours": 1.5,
            "records_touched": 15,
            "error_count": 0,
            "backlog_flag": False,
            "exception_flag": False,
            "sla_breached": False,
        },
    )

    response = client.get("/api/workflows/WF-001")

    assert response.status_code == 200
    assert response.json()["workflow_id"] == "WF-001"


def test_workflow_detail_not_found(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_workflow_detail",
        lambda *args, **kwargs: None,
    )

    response = client.get("/api/workflows/WF-404")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_exception_detail_not_found(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_exceptions",
        lambda *args, **kwargs: ([], 0),
    )

    response = client.get("/api/exceptions/WF-404")

    assert response.status_code == 404


def test_team_performance_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "opspulse.api.routes.repository.fetch_team_performance",
        lambda *args, **kwargs: (
            [
                {
                    "calendar_date": "2025-01-01",
                    "team_name": "PaymentsOps",
                    "workflow_type": "InvoiceApproval",
                    "total_workflows": 75,
                    "completed_workflows": 70,
                    "backlog_workflows": 3,
                    "exception_workflows": 2,
                    "sla_breach_count": 1,
                    "avg_processing_minutes": 25.4,
                    "avg_age_hours": 3.2,
                    "throughput_per_assignee": 8.4,
                    "data_quality_score": 96.0,
                }
            ],
            1,
        ),
    )

    response = client.get("/api/teams/performance")

    assert response.status_code == 200
    assert response.json()["items"][0]["team_name"] == "PaymentsOps"
