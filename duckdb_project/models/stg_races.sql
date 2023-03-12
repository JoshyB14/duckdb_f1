-- Purpose: Cleaned staging model of raw_races
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          13/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import
with input as (
    select * from {{ref('raw_races')}}
),

-- cleaning up column names and taking only needed cols 
staging as (
    select
        raceId as race_id,
        year as year,
        round as round_number,
        circuitId as circuit_id,
        name as race_name,
        date as race_date,
        replace(time,'\N','') as race_time -- remove new line chars
        -- other cols in raw_races are mostly (>90%) new line chars. Hence remove.
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

