select
    protocol,
    count(*) as total_events,
    count(*) filter (where is_attack) as attack_events,
    count(*) filter (where is_failed) as failed_events,
    coalesce(
        round(
            (
                count(*) filter (where is_attack)::numeric
                / nullif(count(*), 0)
            ),
            4
        ),
        0
    ) as attack_rate,
    round(avg(packet_size)::numeric, 2) as avg_packet_size,
    round(avg(duration_ms)::numeric, 2) as avg_duration_ms,
    min(event_timestamp) as first_seen,
    max(event_timestamp) as last_seen
from {{ ref('stg_processed_iot_logs') }}
group by protocol
