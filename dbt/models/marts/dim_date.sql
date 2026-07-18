{{
    config(
        materialized='table'
    )
}}

-- Full-refresh calendar reference table. Not tied to the pipeline's 5-minute
-- SLA; Gold facts depend on complete calendar coverage for partitioning and
-- BI drill-down, so the range below is generated wide and refreshed
-- independently of pipeline runs (see var start_date/end_date to adjust).

with days as (

    select explode(sequence(
        to_date('{{ var("dim_date_start_date", "2015-01-01") }}'),
        to_date('{{ var("dim_date_end_date", "2035-12-31") }}'),
        interval 1 day
    )) as full_date

),

base as (

    select
        full_date,
        cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,
        cast(dayofweek(full_date) as smallint) as day_of_week,
        cast(dayofmonth(full_date) as smallint) as day_of_month,
        cast(dayofyear(full_date) as smallint) as day_of_year,
        cast(weekofyear(full_date) as smallint) as week_of_year,
        cast(month(full_date) as smallint) as month,
        cast(quarter(full_date) as smallint) as quarter,
        cast(year(full_date) as smallint) as year,
        dayofweek(full_date) in (1, 7) as is_weekend
    from days

),

holidays as (

    select
        full_date,
        holiday_name
    from {{ ref('us_holidays') }}

)

select
    base.full_date,
    base.date_key,
    base.day_of_week,
    base.day_of_month,
    base.day_of_year,
    base.week_of_year,
    base.month,
    base.quarter,
    base.year,
    base.is_weekend,
    holidays.holiday_name,
    holidays.holiday_name is not null as is_holiday
from base
left join holidays on base.full_date = holidays.full_date
