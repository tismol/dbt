{{ config(materialized='table') }}

select
    transaction_date,
    us_state,
    count() as transaction_cnt,
    sum(amount) as amount_sum,
    avg(amount) as avg_amount,
    quantile(0.95)(amount) as p95_amount, -- noqa: PRS
    {{ pct('sum(is_large_amount)', 'count()') }} as large_transaction_pct
from {{ ref('stg_transactions') }}
group by
    transaction_date, us_state
