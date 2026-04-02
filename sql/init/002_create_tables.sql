CREATE TABLE IF NOT EXISTS raw.workflow_events_raw (
    raw_event_id BIGSERIAL PRIMARY KEY,
    source_file_name TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    workflow_id TEXT NOT NULL,
    workflow_type TEXT NOT NULL,
    team_name TEXT NOT NULL,
    assignee_id TEXT,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    backlog_flag BOOLEAN NOT NULL DEFAULT FALSE,
    exception_flag BOOLEAN NOT NULL DEFAULT FALSE,
    exception_type TEXT,
    source_system TEXT NOT NULL,
    records_touched INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_file_name, source_row_number)
);

CREATE INDEX IF NOT EXISTS idx_raw_workflow_events_workflow_id
    ON raw.workflow_events_raw (workflow_id);

CREATE INDEX IF NOT EXISTS idx_raw_workflow_events_created_at
    ON raw.workflow_events_raw (created_at);

CREATE TABLE IF NOT EXISTS staging.workflow_events_clean (
    staging_event_id BIGSERIAL PRIMARY KEY,
    raw_event_id BIGINT NOT NULL REFERENCES raw.workflow_events_raw(raw_event_id),
    workflow_id TEXT NOT NULL,
    workflow_type TEXT NOT NULL,
    team_name TEXT NOT NULL,
    assignee_id TEXT,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    processing_minutes NUMERIC(12, 2),
    age_hours NUMERIC(12, 2),
    sla_breached BOOLEAN NOT NULL DEFAULT FALSE,
    backlog_flag BOOLEAN NOT NULL DEFAULT FALSE,
    exception_flag BOOLEAN NOT NULL DEFAULT FALSE,
    exception_type TEXT,
    records_touched INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    validation_status TEXT NOT NULL,
    validation_errors TEXT[] NOT NULL DEFAULT '{}',
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (raw_event_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_workflow_events_workflow_id
    ON staging.workflow_events_clean (workflow_id);

CREATE INDEX IF NOT EXISTS idx_staging_workflow_events_team_status
    ON staging.workflow_events_clean (team_name, status);

CREATE TABLE IF NOT EXISTS warehouse.dim_team (
    team_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE,
    department_name TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_workflow_type (
    workflow_type_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_type TEXT NOT NULL UNIQUE,
    workflow_domain TEXT NOT NULL,
    default_sla_hours INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_priority (
    priority_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    priority_name TEXT NOT NULL UNIQUE,
    priority_rank INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_status (
    status_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status_name TEXT NOT NULL UNIQUE,
    terminal_flag BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    day_of_week SMALLINT NOT NULL,
    day_name TEXT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month_number SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    quarter_number SMALLINT NOT NULL,
    year_number SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_workflow_run (
    workflow_run_key BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    team_key INTEGER NOT NULL REFERENCES warehouse.dim_team(team_key),
    workflow_type_key INTEGER NOT NULL REFERENCES warehouse.dim_workflow_type(workflow_type_key),
    priority_key INTEGER NOT NULL REFERENCES warehouse.dim_priority(priority_key),
    status_key INTEGER NOT NULL REFERENCES warehouse.dim_status(status_key),
    queue_name TEXT NOT NULL,
    source_system TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    processing_minutes NUMERIC(12, 2),
    age_hours NUMERIC(12, 2),
    records_touched INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    backlog_flag BOOLEAN NOT NULL DEFAULT FALSE,
    exception_flag BOOLEAN NOT NULL DEFAULT FALSE,
    sla_breached BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_workflow_run_workflow_id
    ON warehouse.fact_workflow_run (workflow_id);

CREATE INDEX IF NOT EXISTS idx_fact_workflow_run_date_team
    ON warehouse.fact_workflow_run (date_key, team_key);

CREATE TABLE IF NOT EXISTS warehouse.fact_exception (
    exception_key BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    team_key INTEGER NOT NULL REFERENCES warehouse.dim_team(team_key),
    workflow_type_key INTEGER NOT NULL REFERENCES warehouse.dim_workflow_type(workflow_type_key),
    priority_key INTEGER NOT NULL REFERENCES warehouse.dim_priority(priority_key),
    exception_type TEXT NOT NULL,
    status_name TEXT NOT NULL,
    error_count INTEGER NOT NULL DEFAULT 0,
    detected_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    open_flag BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_fact_exception_open_flag
    ON warehouse.fact_exception (open_flag, detected_at);

CREATE TABLE IF NOT EXISTS warehouse.fact_backlog_daily (
    backlog_snapshot_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    team_key INTEGER NOT NULL REFERENCES warehouse.dim_team(team_key),
    workflow_type_key INTEGER NOT NULL REFERENCES warehouse.dim_workflow_type(workflow_type_key),
    priority_key INTEGER NOT NULL REFERENCES warehouse.dim_priority(priority_key),
    open_workflow_count INTEGER NOT NULL,
    overdue_workflow_count INTEGER NOT NULL,
    avg_age_hours NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date_key, team_key, workflow_type_key, priority_key)
);

CREATE TABLE IF NOT EXISTS marts.kpi_daily_summary (
    kpi_summary_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    team_key INTEGER REFERENCES warehouse.dim_team(team_key),
    workflow_type_key INTEGER REFERENCES warehouse.dim_workflow_type(workflow_type_key),
    total_workflows INTEGER NOT NULL,
    completed_workflows INTEGER NOT NULL,
    backlog_workflows INTEGER NOT NULL,
    exception_workflows INTEGER NOT NULL,
    sla_breach_count INTEGER NOT NULL,
    avg_processing_minutes NUMERIC(12, 2) NOT NULL,
    avg_age_hours NUMERIC(12, 2) NOT NULL,
    throughput_per_assignee NUMERIC(12, 2),
    data_quality_score NUMERIC(5, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date_key, team_key, workflow_type_key)
);
