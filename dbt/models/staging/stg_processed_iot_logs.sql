with source_data as (
    select * from {{ source('warehouse', 'processed_iot_logs') }}
)

select
    id,
    event_timestamp,
    {{ to_utc_date('event_timestamp') }} as event_date,
    {{ extract_utc_hour('event_timestamp') }} as event_hour,
    trim(device_id) as device_id,
    trim(source_ip) as source_ip,
    trim(destination_ip) as destination_ip,
    upper(trim(protocol)) as protocol,
    packet_size,
    duration_ms,
    lower(trim(event_type)) as event_type,
    nullif(trim(attack_type), '') as attack_type,
    lower(trim(status)) as status,
    ingestion_timestamp,
    processed_at,
    inserted_at,
    (
        lower(trim(event_type)) = 'attack'
        or nullif(trim(attack_type), '') is not null
    ) as is_attack,
    (lower(trim(status)) in ('failed', 'error')) as is_failed
from source_data
