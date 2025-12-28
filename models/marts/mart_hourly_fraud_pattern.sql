{{ config(materialized='table') }}

select
    transaction_dow,
    transaction_hour,
    count() as transaction_cnt,
    sum(target) as fraud_cnt,
    {{ pct('sum(target)', 'count()') }} as fraud_rate,
    avg(amount) as avg_amount
from {{ ref('stg_transactions') }}
group by
    transaction_dow,
    transaction_hour
order by
    fraud_rate desc,
    fraud_cnt desc,
    transaction_cnt desc
