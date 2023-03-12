-- Purpose: Cleaned staging model of raw_qualifying
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          13/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import
with input as (
    select * from {{ref('raw_qualifying')}}
),

-- cleaning up column names and taking only needed cols 
staging as (
    select
        raceId as race_id,
        driverId as driver_id,
        constructorId as constructor_id
        number as car_number,
        position as final_qualigying_position,
        repalce(q1, '\N','') as q1_time, -- remove new line chars
        repalce(q2, '\N','') as q2_time, -- remove new line chars
        repalce(q3, '\N','') as q3_time -- remove new line chars
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by race_id, driver_id) as rn
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

