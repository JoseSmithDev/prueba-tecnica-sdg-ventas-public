{{ config(materialized='incremental') }}
{{ automate_dv.link(src_pk='HK_CUSTOMER_NATION', src_fk=['HK_CUSTOMER', 'HK_NATION'],
                    src_ldts='LOAD_DTS', src_source='RECORD_SOURCE',
                    source_model='vw_stg_customer') }}
