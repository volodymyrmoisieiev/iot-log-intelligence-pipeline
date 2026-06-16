with source_data as (
    select * from {{ source('warehouse', 'invalid_iot_logs') }}
)

select
    id,
    raw_payload,
    error_reason,
    failed_at,
    inserted_at,
    case
        when lower(error_reason) like 'invalid json%' then 'invalid_json'
        when lower(error_reason) like '%missing required field%' then 'missing_required_field'
        when lower(error_reason) like '%invalid timestamp%' then 'invalid_timestamp'
        when lower(error_reason) like '%invalid protocol%' then 'invalid_protocol'
        when lower(error_reason) like '%packet_size%' then 'invalid_packet_size'
        when lower(error_reason) like '%duration_ms%' then 'invalid_duration'
        when lower(error_reason) like '%event_type%' then 'invalid_event_type'
        else 'other'
    end as error_category
from source_data
