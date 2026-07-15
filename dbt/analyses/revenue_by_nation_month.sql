-- Prueba de explotabilidad del mart: facturación (vl_net_revenue) por nación
-- del cliente y mes de pedido. Solo toma la versión SCD2 vigente del cliente
-- (fl_is_current) para no duplicar líneas por clientes con varias versiones.
select
    dc.ds_nation,
    dd.qt_year,
    dd.qt_month,
    count(distinct f.id_order_line)   as qt_order_lines,
    sum(f.vl_net_revenue)             as vl_total_net_revenue
from {{ ref('tb_fct_order_line') }} f
join {{ ref('tb_dim_customer') }} dc
    on dc.id_customer = f.fk_customer and dc.fl_is_current = true
join {{ ref('tb_dim_date') }} dd
    on dd.id_date = f.fk_order_date
group by dc.ds_nation, dd.qt_year, dd.qt_month
order by dc.ds_nation, dd.qt_year, dd.qt_month
