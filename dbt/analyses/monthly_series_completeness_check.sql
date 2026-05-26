-- Verify all monthly series have no month gaps (uniqueness already verified with generic test)
select
    series_id,
    count(distinct date) as distinct_months,
    datediff('month', min(date), max(date)) + 1 as expected_months
from {{ source('fred', 'fred_observations') }}
where series_id in ('CIVPART', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS')
group by series_id