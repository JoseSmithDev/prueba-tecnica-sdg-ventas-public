{{ config(materialized='incremental') }}
{{ automate_dv.link(src_pk='HK_NATION_REGION', src_fk=['HK_NATION', 'HK_REGION'],
                    src_ldts='LOAD_DTS', src_source='RECORD_SOURCE',
                    source_model='vw_stg_nation') }}
