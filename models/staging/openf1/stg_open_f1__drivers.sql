with source as (
    select * from {{ source('open_f1', 'drivers') }}
),

renamed as (
    select 
        cast(driver_number as int4) as driver_id,
        cast(full_name as varchar(100)) as driver_full_name,
        cast(name_acronym as varchar(3)) as driver_name_acronym,
        cast(team_name as varchar(100)) as driver_team_name,
        cast(team_colour as varchar(10)) as driver_team_colour,
        cast(session_key as int4) as session_key,
        cast(last_updated as timestamp) as last_updated
    from source
)

select * from renamed