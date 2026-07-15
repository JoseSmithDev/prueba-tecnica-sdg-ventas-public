{{ config(materialized='incremental') }}
{{ automate_dv.sat(src_pk='HK_ORDER', src_hashdiff='HD_ORDER',
                   src_payload=['O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_orders') }}
