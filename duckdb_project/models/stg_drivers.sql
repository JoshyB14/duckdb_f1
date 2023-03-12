-- Purpose: Cleaned staging model of raw_drivers
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import data
with input as (
    select * from {{ref('raw_drivers')}}
),

-- cleaning up col names and taking only required cols
staging as (
    select
        driverId as driver_id,
        number as driver_number,
        code as driver_code,
        forename as driver_first_name,
        surname as driver_surname,
        dob as driver_dob,
        nationality as driver_nationality
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by driver_id, driver_first_name, driver_surname) as rn
    from staging
),

-- take only 1st instance of PK's (deduping)
final as (
    select
        *
        exclude(rn)
    from dedup
        where rn = 1
)

select * from final

