CREATE OR REPLACE VIEW marts.v_team_performance_daily AS
SELECT
    d.calendar_date,
    t.team_name,
    wt.workflow_type,
    k.total_workflows,
    k.completed_workflows,
    k.backlog_workflows,
    k.exception_workflows,
    k.sla_breach_count,
    k.avg_processing_minutes,
    k.avg_age_hours,
    k.throughput_per_assignee,
    k.data_quality_score
FROM marts.kpi_daily_summary k
JOIN warehouse.dim_date d
  ON d.date_key = k.date_key
LEFT JOIN warehouse.dim_team t
  ON t.team_key = k.team_key
LEFT JOIN warehouse.dim_workflow_type wt
  ON wt.workflow_type_key = k.workflow_type_key;

CREATE OR REPLACE VIEW marts.v_open_exceptions AS
SELECT
    e.workflow_id,
    d.calendar_date AS detected_date,
    t.team_name,
    wt.workflow_type,
    p.priority_name,
    e.exception_type,
    e.status_name,
    e.error_count,
    e.open_flag
FROM warehouse.fact_exception e
JOIN warehouse.dim_date d
  ON d.date_key = e.date_key
JOIN warehouse.dim_team t
  ON t.team_key = e.team_key
JOIN warehouse.dim_workflow_type wt
  ON wt.workflow_type_key = e.workflow_type_key
JOIN warehouse.dim_priority p
  ON p.priority_key = e.priority_key
WHERE e.open_flag = TRUE;
