{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='transaction_id',
    incremental_strategy='merge',
    tblproperties={
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }
) }}

with raw_stream_changes as (
    -- 🌟 FIXED: Pointing to the clean internal ref macro
    select * from {{ ref('base_raw_igaming_stream') }}
    {% if is_incremental() and not var('is_unit_test', false) %}
        -- 🌟 THE FIX: High-water mark tracking using ingestion timestamps instead of metadata versions
        where ingested_at > (select coalesce(max(bronze_ingested_at), cast('1970-01-01' as timestamp)) from {{ this }})
    {% endif %}
),

processed_mutations as (
    select
        cast(raw_payload:transaction_id as string) as transaction_id,
        cast(raw_payload:app_user_id as string) as app_user_id,
        cast(raw_payload:operator_brand as string) as operator_brand,
        cast(raw_payload:state_code as string) as state_code,
        cast(raw_payload:event_timestamp as timestamp) as activity_timestamp,
        cast(raw_payload:event_timestamp as date) as activity_date,
        {{ hash_pii('raw_payload:user_email::string') }} as user_email_hashed,
        ingested_at as bronze_ingested_at,
        current_timestamp() as silver_processed_at
    from raw_stream_changes

    -- 🌟 THE OPTIMIZATION: Safeguard against multiple mutations of the same transaction landing in the same micro-batch.
    -- This keeps the latest state per transaction ID and prevents Databricks MERGE compilation errors.
    qualify row_number() over (
        partition by cast(raw_payload:transaction_id as string)
        order by cast(raw_payload:event_timestamp as timestamp) desc
    ) = 1
)

select * from processed_mutations