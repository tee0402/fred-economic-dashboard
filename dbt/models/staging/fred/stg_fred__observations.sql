with source as (

    select * from {{ source('fred', 'fred_observations') }}

),

renamed as (

    select
        -- ids
        series_id,

        -- strings
        series_name,

        -- dates
        date,
        
        -- numerics
        value

    from source

)

select * from renamed