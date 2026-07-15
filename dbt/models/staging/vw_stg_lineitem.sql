-- GENERATED FROM metadata/entities/lineitem.yml — no editar a mano,
-- volver a ejecutar `python -m scripts.generate_staging`
{%- set yaml_metadata -%}
source_model: vw_raw_lineitem
derived_columns:
  EFFECTIVE_FROM: TO_DATE(LOAD_ID || '01', 'YYYYMMDD')
hashed_columns:
  HK_LINEITEM:
  - L_ORDERKEY
  - L_PARTKEY
  - L_SUPPKEY
  - L_LINENUMBER
  HK_ORDER: L_ORDERKEY
  HK_PART: L_PARTKEY
  HK_SUPPLIER: L_SUPPKEY
  HD_LINEITEM:
    is_hashdiff: true
    columns:
    - L_QUANTITY
    - L_EXTENDEDPRICE
    - L_DISCOUNT
    - L_TAX
    - L_RETURNFLAG
    - L_LINESTATUS
    - L_SHIPDATE
    - L_COMMITDATE
    - L_RECEIPTDATE
    - L_SHIPINSTRUCT
    - L_SHIPMODE
    - L_COMMENT
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
