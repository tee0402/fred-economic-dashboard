-- Check that there are no mid-month dates
select
    *
from {{ source('fred', 'fred_observations') }}
where date != date_trunc('month', date)