"""Genera la capa raw + staging de dbt a partir de metadata/entities/*.yml.

Por cada entidad habilitada crea dos modelos:
  - dbt/models/raw/vw_raw_<entidad>.sql      vista prefiltro por load_id
  - dbt/models/staging/vw_stg_<entidad>.sql  automate_dv.stage(): hashes + ldts/rsrc

y un `dbt/models/raw/sources.yml` con las 8 tablas BRONZE.LANDING.TB_*.

Uso (desde la raíz del repo, con el venv activado):
    python -m scripts.generate_staging

Los ficheros generados llevan una cabecera "GENERATED FROM ..." — no se editan a
mano, se vuelve a ejecutar este script tras tocar la metadata.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from ingestion.metadata import EntityConfig, load_all_entities

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "dbt" / "models" / "raw"
STAGING_DIR = REPO_ROOT / "dbt" / "models" / "staging"

# Mapa fijo del modelo Data Vault (6 hubs / 6 links / 8 satélites): qué hub hash key
# debe reproducir cada columna FK, con el mismo algoritmo de hash que su hub
# propietario, para que los links puedan enlazar hubs sin un JOIN físico en staging.
FK_TO_HUB_KEY = {
    "n_regionkey": "HK_REGION",
    "c_nationkey": "HK_NATION",
    "s_nationkey": "HK_NATION",
    "o_custkey": "HK_CUSTOMER",
    "ps_partkey": "HK_PART",
    "ps_suppkey": "HK_SUPPLIER",
    "l_orderkey": "HK_ORDER",
    "l_partkey": "HK_PART",
    "l_suppkey": "HK_SUPPLIER",
}


def _hub_suffix(hub_name: str) -> str:
    return hub_name[len("hub_"):].upper()


def _link_suffix(link_name: str) -> str:
    return link_name[len("lnk_"):].upper()


def build_hashed_columns(cfg: EntityConfig) -> dict:
    """Devuelve el dict hashed_columns para automate_dv.stage(), en el orden:
    hash key propio (si hay hub) -> hash keys de links -> hashdiff."""
    dv = cfg.raw["data_vault"]
    hashed_columns: dict = {}
    exclude_from_hd: set[str] = set(dv.get("hashdiff_exclude", []))

    if "hub" in dv:
        own_bk = dv["business_key"]
        suffix = _hub_suffix(dv["hub"])
        hashed_columns[f"HK_{suffix}"] = own_bk[0].upper() if len(own_bk) == 1 else [c.upper() for c in own_bk]
        exclude_from_hd |= set(own_bk)

        for link in dv.get("links", []):
            link_suffix = _link_suffix(link["name"])
            cols = link["columns"]
            hashed_columns[f"HK_{link_suffix}"] = [c.upper() for c in cols]
            for c in cols:
                if c not in own_bk:
                    hashed_columns[FK_TO_HUB_KEY[c]] = c.upper()
                    exclude_from_hd.add(c)

        hd_name = f"HD_{suffix}"
    else:
        link_suffix = _link_suffix(dv["link"])
        link_cols = dv["link_columns"]
        hashed_columns[f"HK_{link_suffix}"] = [c.upper() for c in link_cols]
        for c in link_cols:
            if c in FK_TO_HUB_KEY:
                hashed_columns[FK_TO_HUB_KEY[c]] = c.upper()
        exclude_from_hd |= set(link_cols)
        hd_name = f"HD_{link_suffix}"

    hd_cols = [col["name"].upper() for col in cfg.columns if col["name"] not in exclude_from_hd]
    hashed_columns[hd_name] = {"is_hashdiff": True, "columns": hd_cols}
    return hashed_columns


def render_raw_view(cfg: EntityConfig) -> str:
    return (
        f"-- GENERATED FROM metadata/entities/{cfg.entity}.yml — no editar a mano,\n"
        f"-- volver a ejecutar `python -m scripts.generate_staging`\n"
        f"-- Vista prefiltro: automate_dv.stage() toma un model ref, no un filtro,\n"
        f"-- así que el filtro de load_id vive aquí, no en la capa staging.\n"
        "select *\n"
        f"from {{{{ source('bronze', 'tb_{cfg.entity}') }}}}\n"
        "where load_id = '{{ var(\"load_id\") }}'\n"
    )


def render_staging_view(cfg: EntityConfig) -> str:
    hashed_columns = build_hashed_columns(cfg)
    metadata = {
        "source_model": f"vw_raw_{cfg.entity}",
        "derived_columns": {
            # applied_dts de negocio derivado del load_id, NO current_timestamp
            # (reproducibilidad: relanzar el mismo load_id da el mismo resultado).
            "EFFECTIVE_FROM": "TO_DATE(LOAD_ID || '01', 'YYYYMMDD')",
        },
        "hashed_columns": hashed_columns,
    }
    yaml_block = yaml.safe_dump(metadata, sort_keys=False, default_flow_style=False).rstrip("\n")
    return (
        f"-- GENERATED FROM metadata/entities/{cfg.entity}.yml — no editar a mano,\n"
        f"-- volver a ejecutar `python -m scripts.generate_staging`\n"
        "{%- set yaml_metadata -%}\n"
        f"{yaml_block}\n"
        "{%- endset -%}\n"
        "{% set metadata_dict = fromyaml(yaml_metadata) %}\n"
        "{{ automate_dv.stage(\n"
        "    include_source_columns=true,\n"
        "    source_model=metadata_dict['source_model'],\n"
        "    derived_columns=metadata_dict['derived_columns'],\n"
        "    hashed_columns=metadata_dict['hashed_columns'],\n"
        "    ranked_columns=none\n"
        ") }}\n"
    )


def render_sources_yml(entities: list[EntityConfig]) -> str:
    tables = "\n".join(f"      - name: tb_{cfg.entity}" for cfg in entities)
    return (
        "# GENERATED FROM metadata/entities/*.yml — no editar a mano,\n"
        "# volver a ejecutar `python -m scripts.generate_staging`\n"
        "version: 2\n\n"
        "sources:\n"
        "  - name: bronze\n"
        "    database: BRONZE\n"
        "    schema: LANDING\n"
        "    tables:\n"
        f"{tables}\n"
    )


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    entities = load_all_entities()

    for cfg in entities:
        (RAW_DIR / f"vw_raw_{cfg.entity}.sql").write_text(render_raw_view(cfg), encoding="utf-8")
        (STAGING_DIR / f"vw_stg_{cfg.entity}.sql").write_text(render_staging_view(cfg), encoding="utf-8")

    (RAW_DIR / "sources.yml").write_text(render_sources_yml(entities), encoding="utf-8")

    print(f"Generados {len(entities)} pares raw/staging + sources.yml")
    for cfg in entities:
        print(f"  - {cfg.entity}")


if __name__ == "__main__":
    main()
