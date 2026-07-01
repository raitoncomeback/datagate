-- stg_macro.sql
-- RBI macro data — sourced from MinIO bronze/macro/
-- Note: bronze_macro DuckDB table uses numeric series schema;
-- RBI press release data lives in MinIO enriched layer.
-- This model reads whatever is loaded into bronze_macro.

select
    series_id,
    cast(date as date)                  as date,
    value,
    cast(ingested_at as timestamptz)   as ingested_at,
    current_timestamp                  as _loaded_at

from {{ source('main', 'bronze_macro') }}
where value is not null