"""Valida metadata/entities/*.yml contra metadata/schema/entity.schema.json.

No necesita Snowflake: es pura validación de contrato. Si esto falla, el
loader y el DAG factory fallarían igual (o peor, en silencio) más adelante.
"""
from __future__ import annotations

import copy

import pytest
import yaml

from ingestion.metadata import (
    ENTITIES_DIR,
    MetadataValidationError,
    load_all_entities,
    load_schema,
    validate_entity,
)

SCHEMA = load_schema()

EXPECTED_ENTITIES = {
    "region", "nation", "customer", "supplier", "part", "partsupp", "orders", "lineitem",
}


def _valid_hub_doc() -> dict:
    return {
        "entity": "customer",
        "enabled": True,
        "source": {"pattern": "{source_root}/customer.csv"},
        "target": {"database": "BRONZE", "schema": "LANDING", "table": "TB_CUSTOMER", "load_type": "full"},
        "columns": [{"name": "c_custkey", "type": "NUMBER"}],
        "data_vault": {"hub": "hub_customer", "business_key": ["c_custkey"], "hashdiff_exclude": []},
        "orchestration": {"dag_group": "ventas_monthly", "depends_on": [], "ingest_priority": 10},
    }


def _valid_link_doc() -> dict:
    doc = _valid_hub_doc()
    doc["entity"] = "partsupp"
    doc["data_vault"] = {
        "link": "lnk_part_supplier",
        "link_columns": ["ps_partkey", "ps_suppkey"],
        "hashdiff_exclude": [],
    }
    return doc


@pytest.mark.parametrize("path", sorted(ENTITIES_DIR.glob("*.yml")), ids=lambda p: p.stem)
def test_every_committed_entity_yaml_is_valid(path):
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    validate_entity(doc, SCHEMA)  # no debe lanzar


def test_all_eight_entities_present_and_enabled():
    entities = load_all_entities()
    names = {e.entity for e in entities}
    assert names == EXPECTED_ENTITIES


def test_entities_ordered_by_ingest_priority():
    entities = load_all_entities()
    priorities = [e.ingest_priority for e in entities]
    assert priorities == sorted(priorities)


def test_valid_hub_doc_passes():
    validate_entity(_valid_hub_doc(), SCHEMA)


def test_valid_link_doc_passes():
    validate_entity(_valid_link_doc(), SCHEMA)


def test_hub_and_link_together_is_rejected():
    doc = _valid_hub_doc()
    doc["data_vault"]["link"] = "lnk_should_not_be_here"
    doc["data_vault"]["link_columns"] = ["a", "b"]
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_missing_required_top_level_key_is_rejected():
    doc = _valid_hub_doc()
    del doc["orchestration"]
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_wrong_database_is_rejected():
    doc = _valid_hub_doc()
    doc["target"]["database"] = "SILVER"
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_table_name_without_tb_prefix_is_rejected():
    doc = _valid_hub_doc()
    doc["target"]["table"] = "CUSTOMER"
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_invalid_load_type_is_rejected():
    doc = _valid_hub_doc()
    doc["target"]["load_type"] = "upsert"
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_link_with_single_column_is_rejected():
    """Un link necesita al menos 2 columnas (es una relación, no un hub)."""
    doc = _valid_link_doc()
    doc["data_vault"]["link_columns"] = ["ps_partkey"]
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_unknown_top_level_key_is_rejected():
    doc = _valid_hub_doc()
    doc["unexpected_field"] = "should not be here"
    with pytest.raises(MetadataValidationError):
        validate_entity(doc, SCHEMA)


def test_deepcopy_of_valid_doc_is_still_valid():
    # Guarda contra mutación accidental compartida entre tests.
    doc = copy.deepcopy(_valid_hub_doc())
    validate_entity(doc, SCHEMA)
