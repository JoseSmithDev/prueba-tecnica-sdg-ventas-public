"""Carga y valida la metadata YAML (config.yml + entities/*.yml).

Esto es lo único que sabe leer el "formato" de la metadata. loader.py y
dag_factory.py consumen EntityConfig, nunca YAML crudo.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[1]
METADATA_DIR = REPO_ROOT / "metadata"
ENTITIES_DIR = METADATA_DIR / "entities"
SCHEMA_PATH = METADATA_DIR / "schema" / "entity.schema.json"
CONFIG_PATH = METADATA_DIR / "config.yml"


class MetadataValidationError(Exception):
    """Un YAML de entidad no cumple metadata/schema/entity.schema.json."""


@dataclass(frozen=True)
class EntityConfig:
    """Vista tipada de un YAML de metadata/entities/<entidad>.yml ya validado."""

    raw: dict[str, Any]

    @property
    def entity(self) -> str:
        return self.raw["entity"]

    @property
    def enabled(self) -> bool:
        return self.raw["enabled"]

    @property
    def source_pattern(self) -> str:
        return self.raw["source"]["pattern"]

    @property
    def target_database(self) -> str:
        return self.raw["target"]["database"]

    @property
    def target_schema(self) -> str:
        return self.raw["target"]["schema"]

    @property
    def target_table(self) -> str:
        return self.raw["target"]["table"]

    @property
    def load_type(self) -> str:
        return self.raw["target"]["load_type"]

    @property
    def columns(self) -> list[dict[str, str]]:
        return self.raw["columns"]

    @property
    def depends_on(self) -> list[str]:
        return self.raw["orchestration"]["depends_on"]

    @property
    def ingest_priority(self) -> int:
        return self.raw["orchestration"]["ingest_priority"]

    @property
    def dag_group(self) -> str:
        return self.raw["orchestration"]["dag_group"]


def load_schema() -> dict[str, Any]:
    with SCHEMA_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def load_global_config() -> dict[str, Any]:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_entity(doc: dict[str, Any], schema: Optional[dict[str, Any]] = None) -> None:
    schema = schema or load_schema()
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
    if errors:
        details = "; ".join(f"{list(e.path)}: {e.message}" for e in errors)
        raise MetadataValidationError(f"YAML de entidad inválido: {details}")


def load_entity(path: Path, schema: Optional[dict[str, Any]] = None) -> EntityConfig:
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    validate_entity(doc, schema)
    return EntityConfig(raw=doc)


def load_all_entities(only: Optional[str] = None) -> list[EntityConfig]:
    """Lee y valida metadata/entities/*.yml. Sin --entity, todas las habilitadas,
    ordenadas por ingest_priority (desempate alfabético). Añadir una entidad
    nueva = añadir un fichero aquí; esta función no cambia."""
    schema = load_schema()
    entities = []
    for path in sorted(ENTITIES_DIR.glob("*.yml")):
        cfg = load_entity(path, schema)
        if only and cfg.entity != only:
            continue
        if not cfg.enabled:
            continue
        entities.append(cfg)
    entities.sort(key=lambda e: (e.ingest_priority, e.entity))
    return entities
