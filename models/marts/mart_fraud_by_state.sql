{{ config(materialized='table') }}

select
    us_state,
    count() as transaction_cnt,
    sum(target) as fraud_cnt,
    {{ pct('sum(target)', 'count()') }} as fraud_rate,
    uniqExact(customer_id) as uniq_customers,
    uniqExact(merch) as uniq_merchants,
    sum(amount) as amount_sum,
    sumIf(amount, target = 1) as fraud_amount_sum
from {{ ref('stg_transactions') }}
group by us_state
