{{ config(materialized='table', file_format='delta') }}

with raw_events as (
    select
        operator_brand,
        user_email_hashed,
        app_user_id,
        state_code,
        activity_timestamp
    from {{ ref('igaming_events') }}
),

track_system_mutations as (
    select
        *,
        -- Track geographic movements
        lag(state_code) over (
            partition by operator_brand, user_email_hashed
            order by activity_timestamp
        ) as prev_state,
        -- Track application ID changes (e.g., system migrations)
        lag(app_user_id) over (
            partition by operator_brand, user_email_hashed
            order by activity_timestamp
        ) as prev_app_user_id
    from raw_events
),

filter_to_significant_changes as (
    select *
    from track_system_mutations
    where prev_state is null
       or state_code != prev_state
       or app_user_id != prev_app_user_id  -- Captures the exact moment a migration occurs
),

build_scd2_timeline as (
    select
        operator_brand,
        user_email_hashed,
        app_user_id,
        prev_app_user_id, -- Maps history forward to maintain lineage
        state_code,
        activity_timestamp as valid_from,
        lead(activity_timestamp) over (
            partition by operator_brand, user_email_hashed
            order by activity_timestamp
        ) as valid_to
    from filter_to_significant_changes
)

select
    {{ dbt_utils.generate_surrogate_key(['operator_brand', 'user_email_hashed', 'valid_from']) }} as user_profile_hk,
    operator_brand,
    user_email_hashed,
    app_user_id,
    prev_app_user_id,
    state_code,
    valid_from,
    coalesce(valid_to, cast('9999-12-31 23:59:59' as timestamp)) as valid_to,
    case when valid_to is null then true else false end as is_current
from build_scd2_timeline