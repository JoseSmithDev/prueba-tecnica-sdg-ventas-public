{{ config(materialized='incremental') }}
{{ automate_dv.sat(src_pk='HK_REGION', src_hashdiff='HD_REGION',
                   src_payload=['R_NAME', 'R_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_region') }}
