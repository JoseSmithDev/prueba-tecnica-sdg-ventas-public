{{ config(materialized='incremental') }}
{{ automate_dv.hub(src_pk='HK_NATION', src_nk='N_NATIONKEY', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_nation') }}
