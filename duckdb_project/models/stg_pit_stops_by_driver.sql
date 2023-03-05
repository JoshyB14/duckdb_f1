{{ config(materialized='view')}}

with drivers as (
    select * from main.raw_drivers
),

pit_stops as (
    select * from main.raw_pit_stops
),

final as (
    select * from drivers
    left join pit_stops on pit_stops.driverID = drivers.driverID
)

select * from final
