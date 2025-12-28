SELECT
    transaction_time as transaction_ts,
    toDate(transaction_time) as transaction_date,
    toHour(transaction_time) as transaction_hour,
    toDayOfWeek(transaction_time) as transaction_dow,
    merch,
    category,
    amount,
    name_1,
    name_2,
    gender,
    street,
    post_code,
    one_city,
    us_state,
    lat,
    lon,
    merchant_lat,
    merchant_lon,
    target,
    {{ amount_bucket('amount') }} as amount_bucket,
    if(toFloat64(amount) >= 1000, 1, 0) as is_large_amount,
    {{ dbt_utils.generate_surrogate_key([
          'name_1','name_2','street','post_code','one_city','us_state'
    ]) }} as customer_id
FROM {{ source('fraud', 'transactions') }}
