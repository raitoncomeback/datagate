select
    ticker,
    cast(date as date)                              as date,
    open,
    high,
    low,
    close,
    volume,
    round(high - low, 2)                           as price_range,
    round(((close - open) / open) * 100, 4)        as intraday_pct_change,
    cast(ingested_at as timestamptz)               as ingested_at,
    current_timestamp                              as _loaded_at
from {{ source('main', 'bronze_stocks') }}
where close > 0
  and volume >= 0
  and high >= low