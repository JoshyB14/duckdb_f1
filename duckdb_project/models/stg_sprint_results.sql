-- Purpose: Cleaned staging model of raw_sprint_results
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          13/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import
with input as (
    select * from {{ref('raw_sprint_results')}}
),

-- cleaning up column names and taking only needed cols 
staging as (
    select
        raceId as race_id,
        driverId as driver_id,
        constructorId as constructor_id,
        number as car_number,
        grid as starting_grid_position,
        position as final_position,
        positionOrder as final_rank,
        points as points,
        laps as number_of_laps,
        time as total_time,
        milliseconds as total_time_milliseconds,
        fastestLap as fastest_lap,
        fastestLapTime as fastest_lap_time,
        statusId as status_id
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

