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

with raw_cdf_changes as (
    -- 🌟 FIXED: Pointing to the clean internal ref macro
    select * from {{ ref('base_raw_igaming_stream') }}
    {% if is_incremental() and not var('is_unit_test', false) %}
        where _commit_version > (select coalesce(max(_commit_version), -1) from {{ this }})
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
        _change_type,
        _commit_version,
        _commit_timestamp,
        current_timestamp() as silver_processed_at
    from raw_cdf_changes
    where _change_type in ('insert', 'update_postimage')
)
select * from processed_mutations