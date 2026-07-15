{{ config(materialized='incremental') }}
{{ automate_dv.sat(src_pk='HK_SUPPLIER', src_hashdiff='HD_SUPPLIER',
                   src_payload=['S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_supplier') }}
