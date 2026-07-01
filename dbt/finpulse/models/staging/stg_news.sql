select
    bn.article_id,
    bn.title,
    bn.description,
    bn.source_name,
    cast(bn.published_at as timestamptz)           as published_at,
    cast(bn.ingested_at as timestamptz)            as ingested_at,
    en.sentiment,
    en.confidence,
    en.tickers_mentioned,
    en.topic_tags,
    en.market_implication,
    cast(en.enriched_at as timestamptz)            as enriched_at,
    current_timestamp                              as _loaded_at

from {{ source('main', 'bronze_news') }} bn
inner join {{ source('main', 'enriched_news') }} en
    on bn.article_id = en.article_id
where en.confidence > 0.0