{{ config(materialized='incremental') }}
{{ automate_dv.sat(src_pk='HK_PART', src_hashdiff='HD_PART',
                   src_payload=['P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_part') }}
