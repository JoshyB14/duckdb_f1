-- Purpose: Cleaned staging model of raw_status
--------------------------------------------------------------------------------
-- Version      Date        Author          Comments
-- 1.0          13/3/23     Josh Bryden     Inital Release
--------------------------------------------------------------------------------

-- import
with input as (
    select * from {{ref('raw_status')}}
),

-- cleaning up column names and taking only needed cols 
staging as (
    select
        statusId as status_id,
        status as status_text
    from input
),

-- row number over PK's 
dedup as (
    select
        *,
        row_number() over(partition by status_id, status_text) as rn
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

