-- GENERATED FROM metadata/entities/partsupp.yml — no editar a mano,
-- volver a ejecutar `python -m scripts.generate_staging`
{%- set yaml_metadata -%}
source_model: vw_raw_partsupp
derived_columns:
  EFFECTIVE_FROM: TO_DATE(LOAD_ID || '01', 'YYYYMMDD')
hashed_columns:
  HK_PART_SUPPLIER:
  - PS_PARTKEY
  - PS_SUPPKEY
  HK_PART: PS_PARTKEY
  HK_SUPPLIER: PS_SUPPKEY
  HD_PART_SUPPLIER:
    is_hashdiff: true
    columns:
    - PS_AVAILQTY
    - PS_SUPPLYCOST
    - PS_COMMENT
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
