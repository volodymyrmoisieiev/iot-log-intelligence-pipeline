with processed_summary as (
    select
        count(*) as processed_records,
        max(processed_at) as last_processed_at
    from {{ ref('stg_processed_iot_logs') }}
),
invalid_summary as (
    select
        count(*) as invalid_records,
        max(failed_at) as last_invalid_at
    from {{ ref('stg_invalid_iot_logs') }}
)

select
    processed_summary.processed_records,
    invalid_summary.invalid_records,
    processed_summary.processed_records + invalid_summary.invalid_records as total_records,
    case
        when (processed_summary.processed_records + invalid_summary.invalid_records) = 0 then 0
        else round(
            (
                {{
                    safe_divide(
                        'invalid_summary.invalid_records',
                        'processed_summary.processed_records + invalid_summary.invalid_records'
                    )
                }}
            ),
            4
        )
    end as invalid_rate,
    processed_summary.last_processed_at,
    invalid_summary.last_invalid_at
from processed_summary
cross join invalid_summary
