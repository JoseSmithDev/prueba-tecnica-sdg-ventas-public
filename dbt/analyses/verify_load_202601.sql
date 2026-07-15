-- Paquete de verificación de la carga 202601, disparada con el mismo DAG
-- (ventas_monthly) cambiando solo el parámetro load_id. Cada bloque trae el
-- resultado esperado como comentario, verificado 2026-07-05 contra Snowflake
-- real (no simulado). Ejecutar cada SELECT por separado en Snowsight.

-- (1) hub_customer: total 22, y las 2 claves cuyo primer load_dts pertenece
-- a la carga 202601 (los 2 clientes nuevos).
-- Esperado: total = 22, nuevos_202601 = 2
select
    count(*) as total,
    count_if(load_dts = (select min(load_dts) from silver.vault.hub_customer where c_custkey in (21, 22))) as nuevos_202601
from silver.vault.hub_customer;

-- (2) sat_customer: filas añadidas en la carga 2 = 5 (2 clientes nuevos +
-- 3 con cambio real de atributos). OJO: un diff de texto plano del CSV
-- customer.csv entre 202512 y 202601 muestra 6 filas distintas, pero 3
-- (custkey 1, 12, 17) son solo ruido de formato -- un cero decimal de más en
-- c_acctbal ("2091.20" vs "2091.2"), el mismo valor numérico una vez que la
-- columna aterriza tipada NUMBER(12,2) en BRONZE. El hashdiff de automate_dv
-- correctamente NO dispara ahí. Solo custkey 8, 14, 19 cambiaron de verdad.
-- Esperado: 5 filas totales (2 nuevas + 3 con >1 versión).
select h.c_custkey, count(*) as versiones
from silver.vault.hub_customer h
join silver.vault.sat_customer s on s.hk_customer = h.hk_customer
group by h.c_custkey
having count(*) > 1
order by h.c_custkey;
-- Esperado: 3 filas (custkey 8, 14, 19), cada una con versiones = 2

-- (2b) Los 3 clientes con cambio real, antes y después, lado a lado.
select
    h.c_custkey,
    s.load_dts,
    s.c_address,
    s.c_phone,
    s.c_acctbal,
    s.c_mktsegment,
    s.c_comment
from silver.vault.hub_customer h
join silver.vault.sat_customer s on s.hk_customer = h.hk_customer
where h.c_custkey in (8, 14, 19)
order by h.c_custkey, s.load_dts;

-- (3) hub_order: +110 filas, cero solape de claves con la carga 1.
-- Esperado: total = 210, solape = 0
select count(*) as total from silver.vault.hub_order;

select count(*) as solape
from bronze.landing.tb_orders a
join bronze.landing.tb_orders b
    on a.o_orderkey = b.o_orderkey and a.load_id = '202512' and b.load_id = '202601';

-- (4) hub_nation: +1, nombrar la nación nueva.
-- Esperado: total = 11, nueva = PORTUGAL
select h.n_nationkey, s.n_name, s.load_dts
from silver.vault.hub_nation h
join silver.vault.sat_nation s on s.hk_nation = h.hk_nation
order by s.load_dts desc
limit 1;

-- (5) Idempotencia: relanzar el mismo load_id no inserta filas nuevas en
-- ningún objeto del vault. Ejecutar dos veces `dbt build --vars
-- '{load_id: "202601"}'` (o re-disparar el DAG con el mismo load_id) y
-- comparar counts antes/después -- deben ser idénticos.
-- Esperado: 0 filas nuevas en la segunda ejecución (verificado end-to-end
-- disparando el DAG ventas_monthly dos veces con load_id=202601).

-- (6) tb_dim_customer: las 3 claves con cambio real muestran fl_is_current
-- alternando entre 2 versiones.
-- Esperado: 3 co_customer distintos (8, 14, 19), cada uno con 2 filas,
-- fl_is_current = true solo en la más reciente.
select co_customer, dt_valid_from, dt_valid_to, fl_is_current
from gold.mart.tb_dim_customer
where co_customer in (8, 14, 19)
order by co_customer, dt_valid_from;

-- (7) Resumen de auditoría por load_id.
-- Esperado: 8 filas SUCCESS por load_id (una por entidad), sin FAILED.
select load_id, layer, status, count(*) as filas, sum(rows_loaded) as total_rows_loaded
from meta.control.tb_ventas_load_audit
group by load_id, layer, status
order by load_id, layer, status;
