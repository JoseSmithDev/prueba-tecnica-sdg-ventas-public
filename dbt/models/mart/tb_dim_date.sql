{{ config(materialized='table') }}

-- Generada a partir de las fechas que realmente aparecen en el negocio
-- (fecha de pedido y fechas de envío/compromiso/recepción de línea), no de
-- un generador de calendario arbitrario.
with all_dates as (

    select o_orderdate as dt_full_date from {{ ref('sat_order') }}
    union
    select l_shipdate from {{ ref('sat_lineitem') }}
    union
    select l_commitdate from {{ ref('sat_lineitem') }}
    union
    select l_receiptdate from {{ ref('sat_lineitem') }}

)

select
    to_number(to_char(dt_full_date, 'YYYYMMDD'))                      as id_date,
    dt_full_date                                                      as dt_full_date,
    year(dt_full_date)                                                as qt_year,
    quarter(dt_full_date)                                             as qt_quarter,
    month(dt_full_date)                                               as qt_month,
    day(dt_full_date)                                                 as qt_day,
    monthname(dt_full_date)                                           as ds_month_name,
    dayname(dt_full_date)                                             as ds_day_name,
    case when dayofweek(dt_full_date) in (0, 6) then true else false end as fl_is_weekend
from all_dates
where dt_full_date is not null
