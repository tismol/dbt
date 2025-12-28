{{ config(materialized='table') }}

with base as (
    select
        customer_id,
        count() as transaction_cnt,
        sum(target) as fraud_cnt,
        {{ pct('sum(target)', 'count()') }} as fraud_rate,
        avg(amount) as avg_amount,
        max(transaction_ts) as last_transaction_time,
        min(transaction_ts) as first_transaction_time
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select
    customer_id,
    transaction_cnt,
    fraud_cnt,
    fraud_rate,
    avg_amount,
    first_transaction_time,
    last_transaction_time,
    multiIf(
        fraud_cnt >= 3 or fraud_rate >= 20, 'HIGH',
        fraud_cnt >= 1 and fraud_rate >= 5, 'MEDIUM',
        'LOW'
    ) as risk_level
from base
