-- Investigate null values and their dates
select
    *
from {{ source('fred', 'fred_observations') }}
where value is null