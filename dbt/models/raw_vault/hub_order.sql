{{ config(materialized='incremental') }}
{{ automate_dv.hub(src_pk='HK_ORDER', src_nk='O_ORDERKEY', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_orders') }}
