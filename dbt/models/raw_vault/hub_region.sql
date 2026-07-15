{{ config(materialized='incremental') }}
{{ automate_dv.hub(src_pk='HK_REGION', src_nk='R_REGIONKEY', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_region') }}
