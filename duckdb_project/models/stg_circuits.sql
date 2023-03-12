-- Purpose: Cleaned staging model of raw_circuits
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import data
with input as (
    select * from {{ref('raw_circuits')}}
),

-- cleaning up col names and taking only required cols
staging as (
    select
        circuitId as circuit_id,
        name as circuit_name,
        location as city,
        country as country,
        lat as latiude,
        lng as longitude,
        replace(alt,'\N','') as altitude -- remove new line chars
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by circuit_id, circuit_name) as rn
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