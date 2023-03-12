-- Purpose: Cleaned staging model of raw_constructor_results
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import data
with input as (
    select * from {{ref('raw_constructor_results')}}
),

-- cleaning up col names and taking only required cols
staging as (
    select
        raceId as race_id,
        constructorId as constructor_id,
        points as constructor_points,
        replace(status, '\N','') as status -- remove new line chars
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by race_id, constructor_id) as rn
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