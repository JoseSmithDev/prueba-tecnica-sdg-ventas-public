-- GENERATED FROM metadata/entities/orders.yml — no editar a mano,
-- volver a ejecutar `python -m scripts.generate_staging`
{%- set yaml_metadata -%}
source_model: vw_raw_orders
derived_columns:
  EFFECTIVE_FROM: TO_DATE(LOAD_ID || '01', 'YYYYMMDD')
hashed_columns:
  HK_ORDER: O_ORDERKEY
  HK_ORDER_CUSTOMER:
  - O_ORDERKEY
  - O_CUSTKEY
  HK_CUSTOMER: O_CUSTKEY
  HD_ORDER:
    is_hashdiff: true
    columns:
    - O_ORDERSTATUS
    - O_TOTALPRICE
    - O_ORDERDATE
    - O_ORDERPRIORITY
    - O_CLERK
    - O_SHIPPRIORITY
    - O_COMMENT
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
