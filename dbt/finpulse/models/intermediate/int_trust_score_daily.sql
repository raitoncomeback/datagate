-- Computes daily trust score per source
-- Trust score = pass_rate weighted by failure severity
-- SCHEMA violations weighted worse than STALE (schema drift = bigger problem)

select
    cast(checked_at as date)                            as date,
    source,
    count(*)                                            as records_checked,
    sum(case when passed = true then 1 else 0 end)      as records_passed,
    sum(case when passed = false then 1 else 0 end)     as records_failed,
    round(
        sum(case when passed = true then 1 else 0 end) * 1.0
        / nullif(count(*), 0),
    4)                                                  as pass_rate,
    -- Weighted trust score — schema failures penalised more heavily
    round(
        1.0 - (
            sum(case when failure_code = 'SCHEMA_MISSING_FIELD' then 2.0
                     when failure_code = 'SCHEMA_WRONG_TYPE'    then 2.0
                     when failure_code = 'RANGE_VIOLATION'      then 1.5
                     when failure_code = 'STALE'                then 1.0
                     when failure_code = 'DUPLICATE'            then 0.5
                     else 0.0 end)
            / nullif(count(*) * 2.0, 0)
        ),
    4)                                                  as trust_score,
    -- Circuit breaker flag — block advisor if trust drops below 85%
    case when round(
        sum(case when passed = true then 1 else 0 end) * 1.0
        / nullif(count(*), 0),
    4) < 0.85
    then true else false end                            as is_blocked,
    current_timestamp                                   as _loaded_at

from {{ source('main', 'gate_results') }}
group by cast(checked_at as date), source