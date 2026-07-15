{{ config(materialized='incremental') }}
{{ automate_dv.hub(src_pk='HK_PART', src_nk='P_PARTKEY', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_part') }}
