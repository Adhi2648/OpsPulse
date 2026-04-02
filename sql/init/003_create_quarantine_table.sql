CREATE TABLE IF NOT EXISTS staging.workflow_events_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    source_file_name TEXT NOT NULL,
    source_row_number INTEGER,
    workflow_id TEXT,
    validation_errors TEXT[] NOT NULL DEFAULT '{}',
    raw_payload JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_file_name, source_row_number)
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_quarantine_workflow_id
    ON staging.workflow_events_quarantine (workflow_id);
