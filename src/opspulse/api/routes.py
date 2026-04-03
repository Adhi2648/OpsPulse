from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from opspulse.api.dependencies import get_db
from opspulse.api import repository
from opspulse.api.schemas import (
    BacklogListResponse,
    DailyKpiListResponse,
    ExceptionListResponse,
    HealthResponse,
    KpiSummaryResponse,
    PaginationMeta,
    TeamPerformanceListResponse,
    WorkflowDetailResponse,
    WorkflowExceptionDetailResponse,
)
from opspulse.db.engine import check_database_health


router = APIRouter()


def _pagination_meta(page: int, page_size: int, total: int, returned: int) -> PaginationMeta:
    return PaginationMeta(page=page, page_size=page_size, total=total, returned=returned)


@router.get("/health", response_model=HealthResponse)
def health(db_check: bool = Query(default=True)) -> HealthResponse:
    """Return service health and optional database connectivity status."""
    database_status = "unchecked"
    if db_check:
        try:
            check_database_health()
            database_status = "ok"
        except Exception:
            database_status = "unavailable"
    return HealthResponse(
        status="ok",
        app="opspulse-api",
        database=database_status,
        timestamp=datetime.now(tz=UTC),
    )


@router.get("/api/kpis/summary", response_model=KpiSummaryResponse)
def get_kpi_summary(
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    workflow_type: str | None = None,
    db: Session = Depends(get_db),
) -> KpiSummaryResponse:
    """Return an aggregated KPI summary for the requested filters."""
    payload = repository.fetch_kpi_summary(db, start_date, end_date, team, workflow_type)
    payload["filters"] = {
        "start_date": start_date,
        "end_date": end_date,
        "team": team,
        "workflow_type": workflow_type,
    }
    return KpiSummaryResponse(**payload)


@router.get("/api/kpis/daily", response_model=DailyKpiListResponse)
def get_kpi_daily(
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    workflow_type: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> DailyKpiListResponse:
    """Return paginated daily KPI rows."""
    items, total = repository.fetch_kpi_daily(
        db,
        start_date=start_date,
        end_date=end_date,
        team=team,
        workflow_type=workflow_type,
        page=page,
        page_size=page_size,
    )
    return DailyKpiListResponse(
        items=items,
        pagination=_pagination_meta(page, page_size, total, len(items)),
    )


@router.get("/api/exceptions", response_model=ExceptionListResponse)
def get_exceptions(
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    priority: str | None = None,
    status_name: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> ExceptionListResponse:
    """Return paginated exception rows."""
    items, total = repository.fetch_exceptions(
        db,
        start_date=start_date,
        end_date=end_date,
        team=team,
        priority=priority,
        status=status_name,
        page=page,
        page_size=page_size,
    )
    return ExceptionListResponse(
        items=items,
        pagination=_pagination_meta(page, page_size, total, len(items)),
    )


@router.get("/api/exceptions/{workflow_id}", response_model=WorkflowExceptionDetailResponse)
def get_exceptions_for_workflow(
    workflow_id: str,
    db: Session = Depends(get_db),
) -> WorkflowExceptionDetailResponse:
    """Return open exceptions for a specific workflow identifier."""
    items, _ = repository.fetch_exceptions(db, workflow_id=workflow_id, page=1, page_size=500)
    if not items:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No exceptions found for workflow_id={workflow_id}",
        )
    return WorkflowExceptionDetailResponse(workflow_id=workflow_id, exceptions=items)


@router.get("/api/backlog", response_model=BacklogListResponse)
def get_backlog(
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    priority: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> BacklogListResponse:
    """Return paginated backlog snapshots."""
    items, total = repository.fetch_backlog(
        db,
        start_date=start_date,
        end_date=end_date,
        team=team,
        priority=priority,
        page=page,
        page_size=page_size,
    )
    return BacklogListResponse(
        items=items,
        pagination=_pagination_meta(page, page_size, total, len(items)),
    )


@router.get("/api/workflows/{workflow_id}", response_model=WorkflowDetailResponse)
def get_workflow(workflow_id: str, db: Session = Depends(get_db)) -> WorkflowDetailResponse:
    """Return the latest known warehouse fact row for a workflow identifier."""
    item = repository.fetch_workflow_detail(db, workflow_id)
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow {workflow_id} was not found",
        )
    return WorkflowDetailResponse(**item)


@router.get("/api/teams/performance", response_model=TeamPerformanceListResponse)
def get_team_performance(
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> TeamPerformanceListResponse:
    """Return paginated team performance rows."""
    items, total = repository.fetch_team_performance(
        db,
        start_date=start_date,
        end_date=end_date,
        team=team,
        page=page,
        page_size=page_size,
    )
    return TeamPerformanceListResponse(
        items=items,
        pagination=_pagination_meta(page, page_size, total, len(items)),
    )
