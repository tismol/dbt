{{ config(materialized='table') }}

with agg as (
    select
        merch,
        count() as transaction_cnt,
        sum(amount) as amount_sum,
        sum(target) as fraud_cnt,
        {{ pct('sum(target)', 'count()') }} as fraud_rate
    from {{ ref('stg_transactions') }}
    group by merch
)

select
    merch,
    transaction_cnt,
    amount_sum,
    fraud_cnt,
    fraud_rate,
    if(transaction_cnt >= 200 and fraud_rate >= 10, 1, 0) as fraud_flag
from agg
