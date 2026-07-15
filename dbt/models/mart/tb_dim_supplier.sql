{{ config(materialized='table') }}

-- Vista actual (no SCD2): el satélite ya guarda el histórico, pero para
-- supplier/part el mart no necesita versionar — basta la última fila por
-- hash key. sat_customer sí se versiona en tb_dim_customer porque ese es
-- el que demuestra la historización en la demo (carga 202601).
with supplier_current as (
    select h.hk_supplier, h.s_suppkey, s.s_name, s.s_address, s.s_phone, s.s_acctbal
    from {{ ref('hub_supplier') }} h
    join {{ ref('sat_supplier') }} s on s.hk_supplier = h.hk_supplier
    qualify row_number() over (partition by h.hk_supplier order by s.load_dts desc) = 1
),

current_supplier_nation as (
    select hk_supplier, hk_nation
    from {{ ref('lnk_supplier_nation') }}
    qualify row_number() over (partition by hk_supplier order by load_dts desc) = 1
),

current_nation as (
    select hk_nation, n_name
    from {{ ref('sat_nation') }}
    qualify row_number() over (partition by hk_nation order by load_dts desc) = 1
),

current_nation_region as (
    select hk_nation, hk_region
    from {{ ref('lnk_nation_region') }}
    qualify row_number() over (partition by hk_nation order by load_dts desc) = 1
),

current_region as (
    select hk_region, r_name
    from {{ ref('sat_region') }}
    qualify row_number() over (partition by hk_region order by load_dts desc) = 1
)

select
    sc.hk_supplier   as id_supplier,
    sc.s_suppkey     as co_supplier,
    sc.s_name        as ds_supplier_name,
    sc.s_address     as ds_supplier_address,
    sc.s_phone       as co_supplier_phone,
    sc.s_acctbal     as vl_account_balance,
    n.n_name         as ds_nation,
    r.r_name         as ds_region
from supplier_current sc
left join current_supplier_nation csn on csn.hk_supplier = sc.hk_supplier
left join current_nation n on n.hk_nation = csn.hk_nation
left join current_nation_region cnr on cnr.hk_nation = csn.hk_nation
left join current_region r on r.hk_region = cnr.hk_region
