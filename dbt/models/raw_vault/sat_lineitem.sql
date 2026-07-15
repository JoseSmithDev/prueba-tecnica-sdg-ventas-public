{{ config(materialized='incremental') }}
{{ automate_dv.sat(src_pk='HK_LINEITEM', src_hashdiff='HD_LINEITEM',
                   src_payload=['L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'],
                   src_eff='EFFECTIVE_FROM', src_ldts='LOAD_DTS',
                   src_source='RECORD_SOURCE', source_model='vw_stg_lineitem') }}
