{{ config(materialized='incremental') }}
-- Link-sat: los atributos (stock, coste) pertenecen a la relación part-supplier,
-- no a part ni a supplier por separado (es el patrón textbook de una tabla puente).
{{ automate_dv.sat(src_pk='HK_PART_SUPPLIER', src_hashdiff='HD_PART_SUPPLIER',
                   src_payload=['PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_partsupp') }}
