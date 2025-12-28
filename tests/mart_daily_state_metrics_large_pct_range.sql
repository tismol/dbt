select *
from {{ ref('mart_daily_state_metrics') }}
where large_transaction_pct is null
   or large_transaction_pct < 0
   or large_transaction_pct > 100