-- mart_ticker_intelligence.sql
-- The gold layer — one row per ticker per day
-- Joins stock prices + news sentiment + trust scores + anomaly flags
-- This is the ONLY table the AI advisor reads from
-- Everything upstream exists to make this table trustworthy

select
    -- Identity
    s.ticker,
    s.date,

    -- Price data
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.price_range,
    s.intraday_pct_change,
    s.avg_close_7d,
    s.avg_close_30d,
    round(
        ((s.close - s.avg_close_30d) / nullif(s.avg_close_30d, 0)) * 100,
    4)                                                  as pct_from_30d_avg,

    -- AI-enriched sentiment (market-wide, not ticker-specific yet)
    n.dominant_sentiment                               as market_sentiment,
    n.bullish_score,
    n.bearish_score,
    n.article_count,
    n.avg_confidence                                   as sentiment_confidence,

    -- Anomaly detection
    s.is_anomaly,
    s.anomaly_explanation,

    -- Data trust scores per source
    ts.trust_score                                     as stocks_trust_score,
    ts.is_blocked                                      as stocks_blocked,
    tn.trust_score                                     as news_trust_score,
    tn.is_blocked                                      as news_blocked,

    -- Overall pipeline health
    case
        when ts.is_blocked = true or tn.is_blocked = true
        then false else true
    end                                                as advisor_can_serve,

    -- Metadata
    s._loaded_at

from {{ ref('int_stock_daily') }} s
left join {{ ref('int_news_sentiment_daily') }} n
    on s.date = n.date
left join {{ ref('int_trust_score_daily') }} ts
    on ts.source = 'stocks'
    and ts.date = (
        select max(date) from {{ ref('int_trust_score_daily') }}
        where source = 'stocks'
    )
left join {{ ref('int_trust_score_daily') }} tn
    on tn.source = 'news'
    and tn.date = (
        select max(date) from {{ ref('int_trust_score_daily') }}
        where source = 'news'
    )