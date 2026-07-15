{{ config(materialized='incremental') }}
{{ automate_dv.sat(src_pk='HK_NATION', src_hashdiff='HD_NATION',
                   src_payload=['N_NAME', 'N_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_nation') }}
