-- Aggregates news sentiment scores per day
-- Weighted by confidence so high-confidence articles count more

select
    cast(published_at as date)                          as date,
    count(*)                                            as article_count,
    round(avg(confidence), 4)                           as avg_confidence,
    sum(case when sentiment = 'bullish' then confidence else 0 end)
        / nullif(sum(confidence), 0)                   as bullish_score,
    sum(case when sentiment = 'bearish' then confidence else 0 end)
        / nullif(sum(confidence), 0)                   as bearish_score,
    sum(case when sentiment = 'neutral' then confidence else 0 end)
        / nullif(sum(confidence), 0)                   as neutral_score,
    case
        when sum(case when sentiment = 'bullish' then confidence else 0 end) >
             sum(case when sentiment = 'bearish' then confidence else 0 end)
        then 'bullish'
        when sum(case when sentiment = 'bearish' then confidence else 0 end) >
             sum(case when sentiment = 'bullish' then confidence else 0 end)
        then 'bearish'
        else 'neutral'
    end                                                 as dominant_sentiment,
    current_timestamp                                   as _loaded_at

from {{ ref('stg_news') }}
where confidence > 0.0
group by cast(published_at as date)