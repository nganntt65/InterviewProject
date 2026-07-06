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
        -- 🌟 THE FIX: Clean, inline extraction and casting using the double-colon operator
        raw_payload:transaction_id::string as transaction_id,
        raw_payload:app_user_id::string as app_user_id,
        raw_payload:operator_brand::string as operator_brand,
        raw_payload:state_code::string as state_code,
        raw_payload:event_timestamp::timestamp as activity_timestamp,
        raw_payload:event_timestamp::date as activity_date,
        
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