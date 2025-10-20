{{ config(
    materialized='table',
    schema='DATA_MART'
) }}

-- ==========================================================
-- DIM_TIME
-- Time dimension generated dynamically from TPCH_SF1 data
-- Author: Alejandra
-- ==========================================================

with date_range as (

    -- Get min and max order dates from TPC-H
    select
        min(o_orderdate) as min_date,
        max(o_orderdate) as max_date
    from {{ source('tpch_sf1', 'orders') }}

),

dates as (

    -- Generate one row per day between min and max
    select
    dateadd(day, seq4(), min_date) as full_date
              ---  dateadd(day, n, min_date) as full_date
    from date_range,
        --join numbers on dateadd(day, n, min_date) <= max_date
        table(generator(rowcount => 10000))

        --- table(generator(rowcount => datediff(day, min_date, max_date) + 1))

),

final as (

    select
        to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
        full_date,
        dayofweekiso(full_date) as day_of_week,
        trim(to_char(full_date, 'FMDay')) as day_name,

 ---full_date,
    to_char(full_date, 'YYYY-MM-DD') as date_str,
    to_char(full_date, 'YYYY') as year,
    to_char(full_date, 'MM') as month,
    to_char(full_date, 'Mon') as month_abbr,
    to_char(full_date, 'FMDay') as day_name,
    to_char(full_date, 'D') as day_of_week,



        day(full_date) as day_of_month,
        dayofyear(full_date) as day_of_year,
        weekiso(full_date) as week_of_year,
        month(full_date) as month_number,
        trim(to_char(full_date, 'Month')) as month_name,
        quarter(full_date) as quarter_number,
        concat('Q', quarter(full_date)) as quarter_name,
        year(full_date) as year_number,
        case when dayofweekiso(full_date) in (6,7) then true else false end as is_weekend,
        false as is_holiday,  -- you can enrich later with a holidays table
        date_trunc('month', full_date) as first_day_of_month,
        last_day(full_date) as last_day_of_month,
        date_trunc('quarter', full_date) as first_day_of_quarter,
        dateadd(day, -1, dateadd(quarter, 1, date_trunc('quarter', full_date))) as last_day_of_quarter,
        date_trunc('year', full_date) as first_day_of_year,
        dateadd(day, -1, dateadd(year, 1, date_trunc('year', full_date))) as last_day_of_year,
        current_timestamp() as created_at

,

        -- Formatos básicos
        'ALEJANDRA' AS OTROS,
        to_char(full_date, 'YYYY-MM-DD') as date_str,
        to_char(full_date, 'YYYY') as year,
        to_char(full_date, 'Q') as quarter,
        to_char(full_date, 'MM') as month,
        to_char(full_date, 'Mon') as month_abbr,
        to_char(full_date, 'Month') as month_name,

        -- Día de semana
        to_char(full_date, 'D') as day_of_week_num,
        to_char(full_date, 'DY') as day_abbr,
        to_char(full_date, 'FMDay') as day_name,

        -- Día / semana / año
        to_char(full_date, 'DDD') as day_of_year,
        weekofyear(full_date) as week_of_year,

        -- Indicadores
        case when to_char(full_date, 'D') in ('1','7') then true else false end as is_weekend,
        case when to_char(full_date, 'D') = '1' then true else false end as is_sunday,
        case when to_char(full_date, 'D') = '7' then true else false end as is_saturday,

        -- Combinaciones útiles
        to_char(full_date, 'YYYY-MM') as year_month,
        to_char(full_date, 'YYYY-"Q"Q') as year_quarter,

        -- Inicio / fin de mes
        date_trunc('month', full_date) as month_start_date,
        last_day(full_date) as month_end_date






    from dates

)

select * from final
order by full_date