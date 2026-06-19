select
    protocol,
    count(*) as total_events,
    {{ count_if('is_attack') }} as attack_events,
    {{ count_if('is_failed') }} as failed_events,
    coalesce(
        round(
            (
                {{ safe_divide(count_if('is_attack'), 'count(*)') }}
            ),
            4
        ),
        0
    ) as attack_rate,
    {{ round_numeric('avg(packet_size)', 2) }} as avg_packet_size,
    {{ round_numeric('avg(duration_ms)', 2) }} as avg_duration_ms,
    min(event_timestamp) as first_seen,
    max(event_timestamp) as last_seen
from {{ ref('stg_processed_iot_logs') }}
group by protocol
