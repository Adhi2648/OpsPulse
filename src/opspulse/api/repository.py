from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _pagination(limit: int, page: int) -> tuple[int, int]:
    safe_limit = max(1, min(limit, 500))
    safe_page = max(1, page)
    return safe_limit, (safe_page - 1) * safe_limit


def _rows_to_dicts(result: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in result.mappings().all()]


def fetch_kpi_summary(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    workflow_type: str | None = None,
) -> dict[str, Any]:
    """Return an aggregated KPI summary from the daily KPI mart."""
    query = text(
        """
        SELECT
            COALESCE(SUM(k.total_workflows), 0) AS total_workflows,
            COALESCE(SUM(k.completed_workflows), 0) AS completed_workflows,
            COALESCE(SUM(k.backlog_workflows), 0) AS backlog_workflows,
            COALESCE(SUM(k.exception_workflows), 0) AS exception_workflows,
            COALESCE(SUM(k.sla_breach_count), 0) AS sla_breach_count,
            COALESCE(AVG(k.avg_processing_minutes), 0) AS avg_processing_minutes,
            COALESCE(AVG(k.avg_age_hours), 0) AS avg_age_hours,
            COALESCE(AVG(k.throughput_per_assignee), 0) AS throughput_per_assignee,
            COALESCE(AVG(k.data_quality_score), 0) AS data_quality_score
        FROM marts.kpi_daily_summary k
        JOIN warehouse.dim_date d
          ON d.date_key = k.date_key
        LEFT JOIN warehouse.dim_team t
          ON t.team_key = k.team_key
        LEFT JOIN warehouse.dim_workflow_type wt
          ON wt.workflow_type_key = k.workflow_type_key
        WHERE (:start_date IS NULL OR d.calendar_date >= :start_date)
          AND (:end_date IS NULL OR d.calendar_date <= :end_date)
          AND (:team IS NULL OR t.team_name = :team)
          AND (:workflow_type IS NULL OR wt.workflow_type = :workflow_type)
        """
    )
    row = session.execute(
        query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "team": team,
            "workflow_type": workflow_type,
        },
    ).mappings().one()
    return dict(row)


def fetch_kpi_daily(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    workflow_type: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict[str, Any]], int]:
    """Return paginated daily KPI rows from the reporting mart view."""
    limit, offset = _pagination(page_size, page)
    count_query = text(
        """
        SELECT COUNT(*) AS total
        FROM marts.v_team_performance_daily v
        WHERE (:start_date IS NULL OR v.calendar_date >= :start_date)
          AND (:end_date IS NULL OR v.calendar_date <= :end_date)
          AND (:team IS NULL OR v.team_name = :team)
          AND (:workflow_type IS NULL OR v.workflow_type = :workflow_type)
        """
    )
    query = text(
        """
        SELECT *
        FROM marts.v_team_performance_daily v
        WHERE (:start_date IS NULL OR v.calendar_date >= :start_date)
          AND (:end_date IS NULL OR v.calendar_date <= :end_date)
          AND (:team IS NULL OR v.team_name = :team)
          AND (:workflow_type IS NULL OR v.workflow_type = :workflow_type)
        ORDER BY v.calendar_date DESC, v.team_name, v.workflow_type
        LIMIT :limit OFFSET :offset
        """
    )
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "team": team,
        "workflow_type": workflow_type,
        "limit": limit,
        "offset": offset,
    }
    total = int(session.execute(count_query, params).scalar_one())
    items = _rows_to_dicts(session.execute(query, params))
    return items, total


def fetch_exceptions(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    priority: str | None = None,
    status: str | None = None,
    workflow_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict[str, Any]], int]:
    """Return paginated exception rows from the open exceptions view."""
    limit, offset = _pagination(page_size, page)
    filters = {
        "start_date": start_date,
        "end_date": end_date,
        "team": team,
        "priority": priority,
        "status": status,
        "workflow_id": workflow_id,
        "limit": limit,
        "offset": offset,
    }
    count_query = text(
        """
        SELECT COUNT(*) AS total
        FROM marts.v_open_exceptions v
        WHERE (:start_date IS NULL OR v.detected_date >= :start_date)
          AND (:end_date IS NULL OR v.detected_date <= :end_date)
          AND (:team IS NULL OR v.team_name = :team)
          AND (:priority IS NULL OR v.priority_name = :priority)
          AND (:status IS NULL OR v.status_name = :status)
          AND (:workflow_id IS NULL OR v.workflow_id = :workflow_id)
        """
    )
    query = text(
        """
        SELECT *
        FROM marts.v_open_exceptions v
        WHERE (:start_date IS NULL OR v.detected_date >= :start_date)
          AND (:end_date IS NULL OR v.detected_date <= :end_date)
          AND (:team IS NULL OR v.team_name = :team)
          AND (:priority IS NULL OR v.priority_name = :priority)
          AND (:status IS NULL OR v.status_name = :status)
          AND (:workflow_id IS NULL OR v.workflow_id = :workflow_id)
        ORDER BY v.detected_date DESC, v.workflow_id
        LIMIT :limit OFFSET :offset
        """
    )
    total = int(session.execute(count_query, filters).scalar_one())
    items = _rows_to_dicts(session.execute(query, filters))
    return items, total


def fetch_backlog(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    priority: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict[str, Any]], int]:
    """Return paginated backlog snapshots from the warehouse fact table."""
    limit, offset = _pagination(page_size, page)
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "team": team,
        "priority": priority,
        "limit": limit,
        "offset": offset,
    }
    count_query = text(
        """
        SELECT COUNT(*) AS total
        FROM warehouse.fact_backlog_daily b
        JOIN warehouse.dim_date d ON d.date_key = b.date_key
        JOIN warehouse.dim_team t ON t.team_key = b.team_key
        JOIN warehouse.dim_priority p ON p.priority_key = b.priority_key
        WHERE (:start_date IS NULL OR d.calendar_date >= :start_date)
          AND (:end_date IS NULL OR d.calendar_date <= :end_date)
          AND (:team IS NULL OR t.team_name = :team)
          AND (:priority IS NULL OR p.priority_name = :priority)
        """
    )
    query = text(
        """
        SELECT
            d.calendar_date,
            t.team_name,
            wt.workflow_type,
            p.priority_name,
            b.open_workflow_count,
            b.overdue_workflow_count,
            b.avg_age_hours
        FROM warehouse.fact_backlog_daily b
        JOIN warehouse.dim_date d ON d.date_key = b.date_key
        JOIN warehouse.dim_team t ON t.team_key = b.team_key
        JOIN warehouse.dim_workflow_type wt ON wt.workflow_type_key = b.workflow_type_key
        JOIN warehouse.dim_priority p ON p.priority_key = b.priority_key
        WHERE (:start_date IS NULL OR d.calendar_date >= :start_date)
          AND (:end_date IS NULL OR d.calendar_date <= :end_date)
          AND (:team IS NULL OR t.team_name = :team)
          AND (:priority IS NULL OR p.priority_name = :priority)
        ORDER BY d.calendar_date DESC, t.team_name, wt.workflow_type
        LIMIT :limit OFFSET :offset
        """
    )
    total = int(session.execute(count_query, params).scalar_one())
    items = _rows_to_dicts(session.execute(query, params))
    return items, total


def fetch_workflow_detail(session: Session, workflow_id: str) -> dict[str, Any] | None:
    """Return the latest known warehouse fact row for a workflow identifier."""
    query = text(
        """
        SELECT
            f.workflow_id,
            d.calendar_date,
            t.team_name,
            wt.workflow_type,
            p.priority_name,
            s.status_name,
            f.queue_name,
            f.source_system,
            f.created_at,
            f.started_at,
            f.completed_at,
            f.due_at,
            f.processing_minutes,
            f.age_hours,
            f.records_touched,
            f.error_count,
            f.backlog_flag,
            f.exception_flag,
            f.sla_breached
        FROM warehouse.fact_workflow_run f
        JOIN warehouse.dim_date d ON d.date_key = f.date_key
        JOIN warehouse.dim_team t ON t.team_key = f.team_key
        JOIN warehouse.dim_workflow_type wt ON wt.workflow_type_key = f.workflow_type_key
        JOIN warehouse.dim_priority p ON p.priority_key = f.priority_key
        JOIN warehouse.dim_status s ON s.status_key = f.status_key
        WHERE f.workflow_id = :workflow_id
        ORDER BY f.created_at DESC
        LIMIT 1
        """
    )
    row = session.execute(query, {"workflow_id": workflow_id}).mappings().first()
    return dict(row) if row else None


def fetch_team_performance(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    team: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict[str, Any]], int]:
    """Return paginated team performance rows from the reporting view."""
    limit, offset = _pagination(page_size, page)
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "team": team,
        "limit": limit,
        "offset": offset,
    }
    count_query = text(
        """
        SELECT COUNT(*) AS total
        FROM marts.v_team_performance_daily v
        WHERE (:start_date IS NULL OR v.calendar_date >= :start_date)
          AND (:end_date IS NULL OR v.calendar_date <= :end_date)
          AND (:team IS NULL OR v.team_name = :team)
        """
    )
    query = text(
        """
        SELECT *
        FROM marts.v_team_performance_daily v
        WHERE (:start_date IS NULL OR v.calendar_date >= :start_date)
          AND (:end_date IS NULL OR v.calendar_date <= :end_date)
          AND (:team IS NULL OR v.team_name = :team)
        ORDER BY v.calendar_date DESC, v.team_name, v.workflow_type
        LIMIT :limit OFFSET :offset
        """
    )
    total = int(session.execute(count_query, params).scalar_one())
    items = _rows_to_dicts(session.execute(query, params))
    return items, total
