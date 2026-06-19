select
    coalesce(nullif(trim(attack_type), ''), 'unknown') as attack_type,
    count(*) as total_attack_events,
    count(distinct device_id) as affected_devices,
    {{ round_numeric('avg(packet_size)', 2) }} as avg_packet_size,
    {{ round_numeric('avg(duration_ms)', 2) }} as avg_duration_ms,
    min(event_timestamp) as first_seen,
    max(event_timestamp) as last_seen
from {{ ref('stg_processed_iot_logs') }}
where is_attack
group by coalesce(nullif(trim(attack_type), ''), 'unknown')
