from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    app: str
    database: str
    timestamp: datetime


class PaginationMeta(BaseModel):
    page: int
    page_size: int
    returned: int
    total: int


class PaginatedResponse(BaseModel):
    items: list[Any]
    pagination: PaginationMeta


class KpiSummaryResponse(BaseModel):
    total_workflows: int
    completed_workflows: int
    backlog_workflows: int
    exception_workflows: int
    sla_breach_count: int
    avg_processing_minutes: float
    avg_age_hours: float
    throughput_per_assignee: float
    data_quality_score: float
    filters: dict[str, Any]


class DailyKpiItem(BaseModel):
    calendar_date: date
    team_name: str | None = None
    workflow_type: str | None = None
    total_workflows: int
    completed_workflows: int
    backlog_workflows: int
    exception_workflows: int
    sla_breach_count: int
    avg_processing_minutes: float
    avg_age_hours: float
    throughput_per_assignee: float | None = None
    data_quality_score: float


class DailyKpiListResponse(BaseModel):
    items: list[DailyKpiItem]
    pagination: PaginationMeta


class ExceptionItem(BaseModel):
    workflow_id: str
    detected_date: date
    team_name: str
    workflow_type: str
    priority_name: str
    exception_type: str
    status_name: str
    error_count: int
    open_flag: bool


class ExceptionListResponse(BaseModel):
    items: list[ExceptionItem]
    pagination: PaginationMeta


class WorkflowExceptionDetailResponse(BaseModel):
    workflow_id: str
    exceptions: list[ExceptionItem]


class BacklogItem(BaseModel):
    calendar_date: date
    team_name: str
    workflow_type: str
    priority_name: str
    open_workflow_count: int
    overdue_workflow_count: int
    avg_age_hours: float


class BacklogListResponse(BaseModel):
    items: list[BacklogItem]
    pagination: PaginationMeta


class WorkflowDetailResponse(BaseModel):
    workflow_id: str
    calendar_date: date
    team_name: str
    workflow_type: str
    priority_name: str
    status_name: str
    queue_name: str
    source_system: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    due_at: datetime | None = None
    processing_minutes: float | None = None
    age_hours: float | None = None
    records_touched: int
    error_count: int
    backlog_flag: bool
    exception_flag: bool
    sla_breached: bool


class TeamPerformanceItem(BaseModel):
    calendar_date: date
    team_name: str
    workflow_type: str
    total_workflows: int
    completed_workflows: int
    backlog_workflows: int
    exception_workflows: int
    sla_breach_count: int
    avg_processing_minutes: float
    avg_age_hours: float
    throughput_per_assignee: float | None = None
    data_quality_score: float


class TeamPerformanceListResponse(BaseModel):
    items: list[TeamPerformanceItem]
    pagination: PaginationMeta
