{{ config(materialized='table')}}

with input as (
    select * from read_csv_auto('/Users/joshbryden/Desktop/github/duckdb_f1/data/constructors.csv')
)

select * from input