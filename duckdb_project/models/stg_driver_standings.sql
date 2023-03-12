-- Purpose: Cleaned staging model of raw_driver_standings
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import data
with input as (
    select * from {{ref('raw_driver_standings')}}
),

-- cleaning up col names and taking only required cols
staging as (
    select
        raceId as race_id,
        driverId as driver_id,
        points as points,
        position as position,
        wins as wins
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

