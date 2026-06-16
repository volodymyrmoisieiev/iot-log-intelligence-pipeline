CREATE TABLE IF NOT EXISTS processed_iot_logs (
    id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMPTZ,
    device_id TEXT,
    source_ip TEXT,
    destination_ip TEXT,
    protocol TEXT,
    packet_size INTEGER,
    duration_ms INTEGER,
    event_type TEXT,
    attack_type TEXT,
    status TEXT,
    ingestion_timestamp TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    raw_payload JSONB,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS invalid_iot_logs (
    id BIGSERIAL PRIMARY KEY,
    raw_payload TEXT,
    error_reason TEXT,
    failed_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
