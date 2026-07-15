{{ config(materialized='table') }}

-- Vista actual (no SCD2) — igual criterio que tb_dim_supplier.
select
    h.hk_part          as id_part,
    h.p_partkey        as co_part,
    s.p_name           as ds_part_name,
    s.p_mfgr           as ds_manufacturer,
    s.p_brand          as ds_brand,
    s.p_type           as ds_part_type,
    s.p_size           as qt_part_size,
    s.p_container      as co_container,
    s.p_retailprice    as vl_retail_price
from {{ ref('hub_part') }} h
join {{ ref('sat_part') }} s on s.hk_part = h.hk_part
qualify row_number() over (partition by h.hk_part order by s.load_dts desc) = 1
