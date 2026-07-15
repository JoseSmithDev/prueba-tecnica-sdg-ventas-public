{{ config(materialized='incremental') }}

-- Grano: una fila por línea de pedido (lineitem) — el único grano totalmente
-- aditivo. Los atributos de cabecera del pedido (estado, prioridad) viajan
-- como dimensión degenerada.
with lineitem_facts as (

    select
        li.hk_lineitem,
        li.hk_order,
        li.hk_part,
        li.hk_supplier,
        li.l_linenumber,
        sl.l_quantity,
        sl.l_extendedprice,
        sl.l_discount,
        sl.l_tax,
        sl.l_returnflag,
        sl.l_linestatus,
        sl.l_shipdate,
        sl.load_dts
    from {{ ref('lnk_lineitem') }} li
    join {{ ref('sat_lineitem') }} sl on sl.hk_lineitem = li.hk_lineitem
    {% if is_incremental() %}
    where sl.load_dts > (select coalesce(max(load_dts), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

order_customer as (
    select hk_order, hk_customer
    from {{ ref('lnk_order_customer') }}
),

order_attrs as (
    select h.hk_order, h.o_orderkey, s.o_orderstatus, s.o_orderpriority, s.o_orderdate
    from {{ ref('hub_order') }} h
    join {{ ref('sat_order') }} s on s.hk_order = h.hk_order
    qualify row_number() over (partition by h.hk_order order by s.load_dts desc) = 1
)

select
    lf.hk_lineitem                                              as id_order_line,
    oc.hk_customer                                              as fk_customer,
    lf.hk_supplier                                              as fk_supplier,
    lf.hk_part                                                  as fk_part,
    to_number(to_char(oa.o_orderdate, 'YYYYMMDD'))               as fk_order_date,
    to_number(to_char(lf.l_shipdate, 'YYYYMMDD'))                as fk_ship_date,
    oa.o_orderkey                                                as co_order,
    lf.l_linenumber                                              as qt_line_number,
    oa.o_orderstatus                                             as co_order_status,
    oa.o_orderpriority                                           as co_order_priority,
    lf.l_quantity                                                as qt_quantity,
    lf.l_extendedprice                                           as vl_extended_price,
    lf.l_discount                                                as vl_discount,
    lf.l_tax                                                     as vl_tax,
    lf.l_extendedprice * (1 - lf.l_discount)                     as vl_net_revenue,
    lf.l_extendedprice * (1 - lf.l_discount) * (1 + lf.l_tax)    as vl_charge,
    lf.l_returnflag                                              as co_return_flag,
    lf.l_linestatus                                              as co_line_status,
    lf.load_dts                                                  as load_dts
from lineitem_facts lf
left join order_customer oc on oc.hk_order = lf.hk_order
left join order_attrs oa on oa.hk_order = lf.hk_order
