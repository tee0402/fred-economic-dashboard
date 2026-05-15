with

observations as (

    select * from {{ ref('stg_fred__observations') }}

),

pivot_and_align_dates_to_monthly_grain as (

    select
        date as month,
        max(case when series_id = 'UNRATE' then value end) as unemployment_rate,
        max(case when series_id = 'CIVPART' then value end) as labor_force_participation_rate,
        max(case when series_id = 'CPIAUCSL' then value end) as consumer_price_index,
        max(case when series_id = 'A191RL1Q225SBEA' then value end) as real_gdp_growth_rate,
        max(case when series_id = 'FEDFUNDS' then value end) as federal_funds_effective_rate,
        max(case when series_id = 'MEHOINUSA672N' then value end) as real_median_household_income

    from observations
    group by 1
    order by 1

)

select * from pivot_and_align_dates_to_monthly_grain