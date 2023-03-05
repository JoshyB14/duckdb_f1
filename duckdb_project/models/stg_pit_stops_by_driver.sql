{{ config(materialized='view')}}

with drivers as (
    select * from main.raw_drivers
),

pit_stops as (
    select * from main.raw_pit_stops
),

final as (
    select * from pit_stops
    left join drivers on pit_stops.driverID = drivers.driverID
)

select * from final
