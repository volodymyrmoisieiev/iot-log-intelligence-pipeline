with device_events as (
    select
        device_id,
        count(*) as total_events,
        count(*) filter (where is_attack) as attack_events,
        count(*) filter (where is_failed) as failed_events,
        count(*) filter (where event_type = 'normal') as normal_events,
        count(*) filter (where event_type = 'warning') as warning_events,
        count(*) filter (where event_type = 'error') as error_events,
        round(
            (
                count(*) filter (where is_attack)::numeric
                / nullif(count(*), 0)
            ),
            4
        ) as attack_rate,
        round(
            (
                count(*) filter (where is_failed)::numeric
                / nullif(count(*), 0)
            ),
            4
        ) as failure_rate,
        round(avg(packet_size)::numeric, 2) as avg_packet_size,
        round(avg(duration_ms)::numeric, 2) as avg_duration_ms,
        min(event_timestamp) as first_seen,
        max(event_timestamp) as last_seen
    from {{ ref('stg_processed_iot_logs') }}
    group by device_id
)

select
    device_id,
    total_events,
    attack_events,
    failed_events,
    normal_events,
    warning_events,
    error_events,
    coalesce(attack_rate, 0) as attack_rate,
    coalesce(failure_rate, 0) as failure_rate,
    avg_packet_size,
    avg_duration_ms,
    first_seen,
    last_seen,
    case
        when coalesce(attack_rate, 0) >= 0.50 or coalesce(failure_rate, 0) >= 0.50 then 'HIGH'
        when coalesce(attack_rate, 0) >= 0.20 or coalesce(failure_rate, 0) >= 0.20 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from device_events
