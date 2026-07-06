{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='sync_all_columns'
) }}

select
    -- 1. Source Columns (adjust explicit columns here if not using select *)
    *,
    -- 2. Audit Metadata Tracking
    current_timestamp() as dbt_updated_at
from {{ source('bronze_lakehouse', 'raw_igaming_stream') }}

{% if is_incremental() %}
    -- 3. High-Water Mark Filter: Only process data newer than our last execution
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}