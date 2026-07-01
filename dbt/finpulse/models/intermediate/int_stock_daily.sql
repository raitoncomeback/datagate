-- Daily stock summary with moving averages
-- Joins anomaly flags where detected

select
    s.ticker,
    s.date,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.price_range,
    s.intraday_pct_change,
    avg(s2.close) over (
        partition by s.ticker
        order by s.date
        rows between 6 preceding and current row
    )                                                   as avg_close_7d,
    avg(s2.close) over (
        partition by s.ticker
        order by s.date
        rows between 29 preceding and current row
    )                                                   as avg_close_30d,
    case when a.ticker is not null then true
         else false end                                 as is_anomaly,
    a.gemini_explanation                                as anomaly_explanation,
    current_timestamp                                   as _loaded_at

from {{ ref('stg_stocks') }} s
left join {{ ref('stg_stocks') }} s2
    on s.ticker = s2.ticker
left join {{ source('main', 'stock_anomalies') }} a
    on s.ticker = a.ticker
    and s.date = cast(a.date as date)