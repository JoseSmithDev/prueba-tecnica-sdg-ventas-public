{{ config(materialized='table') }}

-- SCD2: el satélite YA es el histórico Type-2 de Data Vault; aquí solo se
-- re-etiqueta con dt_valid_from/dt_valid_to/fl_is_current para consumo BI.
-- Tras la carga 202601, los 6 clientes modificados muestran 2 versiones aquí.
with customer_history as (

    select
        h.hk_customer,
        h.c_custkey,
        s.c_name,
        s.c_address,
        s.c_phone,
        s.c_acctbal,
        s.c_mktsegment,
        s.load_dts
    from {{ ref('hub_customer') }} h
    join {{ ref('sat_customer') }} s on s.hk_customer = h.hk_customer

),

-- Sin satélite de efectividad en el link, la "nación actual" se resuelve
-- tomando la última fila del link por hk_customer (limitación conocida y
-- asumida: bastaría un effectivity satellite para historizar la relación).
current_customer_nation as (
    select hk_customer, hk_nation
    from {{ ref('lnk_customer_nation') }}
    qualify row_number() over (partition by hk_customer order by load_dts desc) = 1
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
    ch.hk_customer                                                                    as id_customer,
    ch.c_custkey                                                                      as co_customer,
    ch.c_name                                                                         as ds_customer_name,
    ch.c_address                                                                      as ds_customer_address,
    ch.c_phone                                                                        as co_customer_phone,
    ch.c_acctbal                                                                      as vl_account_balance,
    ch.c_mktsegment                                                                   as co_market_segment,
    n.n_name                                                                          as ds_nation,
    r.r_name                                                                          as ds_region,
    ch.load_dts                                                                       as dt_valid_from,
    lead(ch.load_dts) over (partition by ch.hk_customer order by ch.load_dts)         as dt_valid_to,
    case when lead(ch.load_dts) over (partition by ch.hk_customer order by ch.load_dts) is null
         then true else false end                                                    as fl_is_current
from customer_history ch
left join current_customer_nation ccn on ccn.hk_customer = ch.hk_customer
left join current_nation n on n.hk_nation = ccn.hk_nation
left join current_nation_region cnr on cnr.hk_nation = ccn.hk_nation
left join current_region r on r.hk_region = cnr.hk_region
