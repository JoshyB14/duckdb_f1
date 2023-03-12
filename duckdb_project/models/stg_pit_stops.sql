-- Purpose: Cleaned staging model of raw_pit_stops
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import
with pit_stops as (
    select * from {{ref('raw_pit_stops')}}
),

-- cleaning up column names and taking only needed cols 
staging as (
    select
        raceId as race_id,
        driverId as driver_id,
        stop as stop_number,
        lap as lap_number,
        time as local_time,
        duration as pit_stop_duration_seconds,
        milliseconds as pit_stop_duration_milliseconds
    from pit_stops
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by race_id,
                                        driver_id,
                                        stop_number,
                                        lap_number) as rn
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
