{% macro to_utc_date(expression) -%}
  {{ return(adapter.dispatch('to_utc_date', 'iot_log_intelligence_pipeline')(expression)) }}
{%- endmacro %}

{% macro default__to_utc_date(expression) -%}
  cast(({{ expression }} at time zone 'UTC') as date)
{%- endmacro %}

{% macro snowflake__to_utc_date(expression) -%}
  to_date(convert_timezone('UTC', {{ expression }}))
{%- endmacro %}

{% macro extract_utc_hour(expression) -%}
  {{ return(adapter.dispatch('extract_utc_hour', 'iot_log_intelligence_pipeline')(expression)) }}
{%- endmacro %}

{% macro default__extract_utc_hour(expression) -%}
  cast(extract(hour from {{ expression }} at time zone 'UTC') as integer)
{%- endmacro %}

{% macro snowflake__extract_utc_hour(expression) -%}
  cast(extract(hour from convert_timezone('UTC', {{ expression }})) as integer)
{%- endmacro %}

{% macro as_numeric(expression) -%}
  cast({{ expression }} as numeric)
{%- endmacro %}

{% macro round_numeric(expression, scale) -%}
  round({{ as_numeric(expression) }}, {{ scale }})
{%- endmacro %}

{% macro count_if(condition) -%}
  sum(case when {{ condition }} then 1 else 0 end)
{%- endmacro %}

{% macro safe_divide(numerator, denominator) -%}
  {{ as_numeric(numerator) }} / nullif({{ as_numeric(denominator) }}, 0)
{%- endmacro %}
