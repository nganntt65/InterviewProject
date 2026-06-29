{{ config(
    materialized='view',
    schema='silver'
) }}

with source_data as (
    {% if var('is_unit_test', false) %}
        -- For unit tests, we manually construct the schema layout dbt expects
        select
            cast(null as string) as raw_payload,
            cast(null as timestamp) as ingested_at,
            cast(null as string) as _change_type,
            cast(null as bigint) as _commit_version,
            cast(null as timestamp) as _commit_timestamp
        where 1 = 0

    {% elif is_incremental() %}
        select * from table_changes(
            '{{ source("bronze_lakehouse", "raw_igaming_stream") }}',
            '{{ var("start_time", "2026-06-01 00:00:00") }}'
        )
    {% else %}
        select * from table_changes(
            '{{ source("bronze_lakehouse", "raw_igaming_stream") }}',
            0
        )
    {% endif %}
)
select * from source_data