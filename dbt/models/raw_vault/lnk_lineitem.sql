{{ config(materialized='incremental') }}
-- l_linenumber viaja como columna del link (dependent child key: distingue
-- líneas dentro del mismo pedido, no es una business key de negocio real).
{{ automate_dv.link(src_pk='HK_LINEITEM', src_fk=['HK_ORDER', 'HK_PART', 'HK_SUPPLIER', 'L_LINENUMBER'],
                    src_ldts='LOAD_DTS', src_source='RECORD_SOURCE',
                    source_model='vw_stg_lineitem') }}
