{{ config(materialized='table')}}

with input as (
    select * from read_csv_auto('/Users/joshbryden/Desktop/github/duckdb_f1/data/lap_times.csv')
)

select * from input