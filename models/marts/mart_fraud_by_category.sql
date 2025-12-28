{{ config(materialized='table') }}

select
    category,
    count() as transaction_cnt,
    sum(target) as fraud_cnt,
    {{ pct('sum(target)', 'count()') }} as fraud_rate,
    sum(amount) as amount_sum,
    sumIf(amount, target = 1) as fraud_amount_sum
from {{ ref('stg_transactions') }}
group by category
