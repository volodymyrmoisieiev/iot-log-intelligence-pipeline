CREATE TABLE IF NOT EXISTS pipeline_run_audit (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    environment TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    processed_records BIGINT NOT NULL DEFAULT 0,
    invalid_records BIGINT NOT NULL DEFAULT 0,
    invalid_rate NUMERIC(12,6),
    high_risk_devices BIGINT NOT NULL DEFAULT 0,
    total_alerts BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_quality_checks (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_status TEXT NOT NULL,
    severity TEXT NOT NULL,
    metric_name TEXT,
    metric_value NUMERIC(18,6),
    threshold_value NUMERIC(18,6),
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    alert_level TEXT NOT NULL,
    alert_message TEXT NOT NULL,
    source TEXT NOT NULL,
    is_published_to_kafka BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_run_id
    ON pipeline_run_audit (run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_status
    ON pipeline_run_audit (status);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_started_at
    ON pipeline_run_audit (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_finished_at
    ON pipeline_run_audit (finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_audit_created_at
    ON pipeline_run_audit (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_quality_checks_run_id
    ON pipeline_quality_checks (run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_quality_checks_check_status
    ON pipeline_quality_checks (check_status);

CREATE INDEX IF NOT EXISTS idx_pipeline_quality_checks_severity
    ON pipeline_quality_checks (severity);

CREATE INDEX IF NOT EXISTS idx_pipeline_quality_checks_created_at
    ON pipeline_quality_checks (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_run_id
    ON pipeline_alerts (run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_alert_level
    ON pipeline_alerts (alert_level);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_is_published_to_kafka
    ON pipeline_alerts (is_published_to_kafka);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_created_at
    ON pipeline_alerts (created_at DESC);
