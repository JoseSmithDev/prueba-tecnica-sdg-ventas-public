-- GENERATED FROM metadata/entities/region.yml — no editar a mano,
-- volver a ejecutar `python -m scripts.generate_staging`
-- Vista prefiltro: automate_dv.stage() toma un model ref, no un filtro,
-- así que el filtro de load_id vive aquí, no en la capa staging.
select *
from {{ source('bronze', 'tb_region') }}
where load_id = '{{ var("load_id") }}'
