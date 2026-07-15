{{ config(materialized='incremental') }}
{{ automate_dv.link(src_pk='HK_PART_SUPPLIER', src_fk=['HK_PART', 'HK_SUPPLIER'],
                    src_ldts='LOAD_DTS', src_source='RECORD_SOURCE',
                    source_model='vw_stg_partsupp') }}
