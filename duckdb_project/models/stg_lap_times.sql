-- Purpose: Cleaned staging model of raw_lap_times
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import data
with input as (
    select * from {{ref('raw_lap_times')}}
),

-- cleaning up col names and taking only required cols
staging as (
    select
        raceId as race_id,
        driverId as driver_id,
        lap as lap_number,
        position as position,
        time as lap_time,
        milliseconds as lap_time_milliseconds
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by race_id, driver_id, lap_number) as rn
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

