"""Task 'verify' del DAG: última pieza del pipeline. Falla el run si alguna
entidad ingerida en este (load_id, run_id) no quedó en SUCCESS en
META.CONTROL.TB_VENTAS_LOAD_AUDIT (tabla escrita por ingestion/loader.py)."""
from __future__ import annotations

import os

import snowflake.connector

from ingestion.metadata import load_all_entities, load_global_config


def _connect() -> "snowflake.connector.SnowflakeConnection":
    """Rol/warehouse de dbt (ROLE_VENTAS_TRANSFORMER_FR): tiene META_READ,
    suficiente para leer la tabla de auditoría — no hace falta el rol de carga."""
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        role=os.environ.get("SF_ROLE", "ROLE_VENTAS_TRANSFORMER_FR"),
        warehouse=os.environ.get("SF_WAREHOUSE", "WH_VENTAS_DBT_XS"),
    )


def check_load_audit(load_id: str, run_id: str) -> None:
    global_cfg = load_global_config()
    d = global_cfg["defaults"]
    audit_table = f"{d['audit_database']}.{d['audit_schema']}.{d['audit_table']}"
    expected = {e.entity for e in load_all_entities()}

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            select entity, status
            from {audit_table}
            where load_id = %(load_id)s
              and airflow_run_id = %(run_id)s
              and layer = 'INGEST'
            """,
            {"load_id": load_id, "run_id": run_id},
        )
        rows = {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()

    missing = expected - rows.keys()
    failed = {entity for entity, status in rows.items() if status != "SUCCESS"}
    if missing or failed:
        raise RuntimeError(
            f"Verificación fallida para load_id={load_id} run_id={run_id}: "
            f"sin fila de auditoría={sorted(missing)}, status != SUCCESS={sorted(failed)}"
        )
