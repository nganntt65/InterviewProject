{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='activity_id',
    incremental_strategy='merge',
    partition_by=['activity_date']
) }}

with new_or_updated_silver as (
    select *
    from {{ ref('igaming_events') }}
    {% if is_incremental() %}
      -- CRITICAL FIX: Only grab rows that have been processed in Silver since the last Gold run
      where silver_processed_at >= (select max(f.silver_processed_at) from {{ this }} f)
    {% endif %}
),

dim_users as (
    select * from {{ ref('dim_users_scd2') }}
),

temporal_profile_match as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.transaction_id', 's.operator_brand']) }} as activity_id,
        s.operator_brand,
        s.user_email_hashed,
        s.app_user_id as current_app_user_id,
        u.prev_app_user_id,
        s.state_code as historical_state_code,
        s.activity_timestamp,
        s.activity_date,

        -- Pass the silver processing token downstream
        s.silver_processed_at,
        -- Record exactly when this execution engine touched the data
        current_timestamp() as fact_processed_at
    from new_or_updated_silver s
    join dim_users u
        on s.operator_brand = u.operator_brand
        and s.user_email_hashed = u.user_email_hashed
        and s.activity_timestamp >= u.valid_from
        and s.activity_timestamp < u.valid_to
)

select * from temporal_profile_match