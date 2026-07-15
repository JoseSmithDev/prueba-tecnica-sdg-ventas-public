{{ config(materialized='incremental') }}
{{ automate_dv.hub(src_pk='HK_SUPPLIER', src_nk='S_SUPPKEY', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_supplier') }}
