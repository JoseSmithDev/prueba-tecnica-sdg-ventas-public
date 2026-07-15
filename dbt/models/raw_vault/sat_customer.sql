{{ config(materialized='incremental') }}
-- Idempotencia: automate_dv compara el HD_CUSTOMER entrante contra el último
-- hashdiff guardado por HK_CUSTOMER (ventana LAG por src_pk, orden src_ldts/src_eff).
-- Si coinciden, la fila no se inserta -> relanzar el mismo load_id inserta 0 filas.
-- Si difiere (6 clientes de la carga 202601), se inserta una versión nueva sin tocar
-- las anteriores: eso ES la historización Data Vault, sin UPDATE ni DELETE.
{{ automate_dv.sat(src_pk='HK_CUSTOMER', src_hashdiff='HD_CUSTOMER',
                   src_payload=['C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_customer') }}
