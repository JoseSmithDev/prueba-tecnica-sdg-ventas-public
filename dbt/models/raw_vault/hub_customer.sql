{{ config(materialized='incremental') }}
{{ automate_dv.hub(src_pk='HK_CUSTOMER', src_nk='C_CUSTKEY', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_customer') }}
