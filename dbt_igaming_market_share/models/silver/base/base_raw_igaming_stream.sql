{{ config(
    materialized='incremental',
    incremental_strategy='append', -- 🌟 THE FIX: Switch to append-only for zero merge overhead
    file_format='delta',
    on_schema_change='append_new_columns'
) }}

select
    -- 1. Source Columns (adjust explicit columns here if not using select *)
    raw_payload,
    ingested_at,
    -- 2. Audit Metadata Tracking
    current_timestamp() as dbt_updated_at
from {{ source('bronze_lakehouse', 'raw_igaming_stream') }}

{% if is_incremental() %}
    -- 3. High-Water Mark Filter: Only process data newer than our last execution
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}