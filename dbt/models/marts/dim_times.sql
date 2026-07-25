{{
    config(
        materialized='table'
    )
}}

-- Static 24-hour reference table (86,400 rows, one per second).
-- Rebuilt on demand, not on every pipeline run; facts bucket into
-- 5-minute intervals without needing this dimension rebuilt each time.

with seconds as (

    select explode(sequence(0, 86399)) as second_of_day

),

base as (

    select
        cast(
            floor(second_of_day / 3600) * 10000
            + floor(mod(second_of_day, 3600) / 60) * 100
            + mod(second_of_day, 60)
        as int) as time_key
    from seconds

),

enriched as (

    select
        time_key,
        cast(floor(time_key / 10000) as smallint) as hour_24,
        cast(
            case
                when mod(floor(time_key / 10000), 12) = 0 then 12
                else mod(floor(time_key / 10000), 12)
            end
        as smallint) as hour_12,
        case when floor(time_key / 10000) < 12 then 'AM' else 'PM' end as am_pm,
        cast(floor(mod(time_key, 10000) / 100) as smallint) as minute,
        cast(mod(time_key, 100) as smallint) as second,
        {{ time_bucket('time_key', 15) }} as time_bucket_15min,
        {{ time_bucket('time_key', 30) }} as time_bucket_30min,
        {{ time_bucket('time_key', 60) }} as time_bucket_hourly,
        case
            when time_key between 0 and 59999 then 'EARLY NIGHT'
            when time_key between 60000 and 119999 then 'MORNING'
            when time_key between 120000 and 179999 then 'AFTERNOON'
            when time_key between 180000 and 219999 then 'EVENING'
            when time_key between 220000 and 235959 then 'NIGHT'
        end as day_part
    from base

)

select
    time_key,
    concat(
        lpad(cast(hour_24 as int), 2, '0'), ':',
        lpad(cast(minute as int), 2, '0'), ':',
        lpad(cast(second as int), 2, '0')
    ) as time_24h,
    hour_24,
    hour_12,
    am_pm,
    minute,
    second,
    time_bucket_15min,
    time_bucket_30min,
    time_bucket_hourly,
    {{ display_bucket_time('time_bucket_15min') }} as time_bucket_15min_str,
    {{ display_bucket_time('time_bucket_30min') }} as time_bucket_30min_str,
    {{ display_bucket_time('time_bucket_hourly') }} as time_bucket_hourly_str,
    day_part
from enriched
