{{ config(materialized='incremental') }}
{{ automate_dv.link(src_pk='HK_ORDER_CUSTOMER', src_fk=['HK_ORDER', 'HK_CUSTOMER'],
                    src_ldts='LOAD_DTS', src_source='RECORD_SOURCE',
                    source_model='vw_stg_orders') }}
