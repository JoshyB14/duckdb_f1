-- Purpose: Cleaned staging model of raw_constructors
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          12/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import data
with input as (
    select * from {{ref('raw_constructors')}}
),

-- cleaning up col names and taking only required cols
staging as (
    select
        constructorId as constructor_id,
        name as constructor_name,
        nationality as constructor_nationality
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by constructor_id, constructor_name) as rn
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

