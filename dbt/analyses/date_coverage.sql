-- Investigate the date range and row count for each series
select
    series_id,
    min(date) as earliest,
    max(date) as latest,
    count(*) as row_count
from {{ source('fred', 'fred_observations') }}
group by 1
order by 2