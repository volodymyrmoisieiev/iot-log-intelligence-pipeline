CREATE TABLE IF NOT EXISTS iot_anomalies (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_row_id TEXT,
    event_timestamp TIMESTAMP NULL,
    device_id TEXT,
    rule_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    reason TEXT NOT NULL,
    score NUMERIC,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iot_anomalies_run_id
    ON iot_anomalies (run_id);

CREATE INDEX IF NOT EXISTS idx_iot_anomalies_device_id
    ON iot_anomalies (device_id);

CREATE INDEX IF NOT EXISTS idx_iot_anomalies_rule_name
    ON iot_anomalies (rule_name);

CREATE INDEX IF NOT EXISTS idx_iot_anomalies_severity
    ON iot_anomalies (severity);

CREATE INDEX IF NOT EXISTS idx_iot_anomalies_created_at
    ON iot_anomalies (created_at DESC);
