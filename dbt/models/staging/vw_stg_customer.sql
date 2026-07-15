-- GENERATED FROM metadata/entities/customer.yml — no editar a mano,
-- volver a ejecutar `python -m scripts.generate_staging`
{%- set yaml_metadata -%}
source_model: vw_raw_customer
derived_columns:
  EFFECTIVE_FROM: TO_DATE(LOAD_ID || '01', 'YYYYMMDD')
hashed_columns:
  HK_CUSTOMER: C_CUSTKEY
  HK_CUSTOMER_NATION:
  - C_CUSTKEY
  - C_NATIONKEY
  HK_NATION: C_NATIONKEY
  HD_CUSTOMER:
    is_hashdiff: true
    columns:
    - C_NAME
    - C_ADDRESS
    - C_PHONE
    - C_ACCTBAL
    - C_MKTSEGMENT
    - C_COMMENT
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
