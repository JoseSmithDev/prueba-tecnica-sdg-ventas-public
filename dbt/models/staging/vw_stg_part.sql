-- GENERATED FROM metadata/entities/part.yml — no editar a mano,
-- volver a ejecutar `python -m scripts.generate_staging`
{%- set yaml_metadata -%}
source_model: vw_raw_part
derived_columns:
  EFFECTIVE_FROM: TO_DATE(LOAD_ID || '01', 'YYYYMMDD')
hashed_columns:
  HK_PART: P_PARTKEY
  HD_PART:
    is_hashdiff: true
    columns:
    - P_NAME
    - P_MFGR
    - P_BRAND
    - P_TYPE
    - P_SIZE
    - P_CONTAINER
    - P_RETAILPRICE
    - P_COMMENT
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
