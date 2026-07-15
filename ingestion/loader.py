"""Motor genérico de ingesta: PUT + COPY INTO, 100% metadata-driven.

Uso:
    python -m ingestion.loader --load-id 202512 [--entity customer]

Este fichero NO contiene lógica de ninguna entidad concreta: el destino, las
columnas, el tipo de carga y las dependencias salen de metadata/entities/*.yml
y metadata/config.yml. Añadir una tabla nueva = añadir un YAML, cero cambios
aquí (ver metadata/schema/entity.schema.json para el contrato).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import snowflake.connector

from ingestion.metadata import REPO_ROOT, EntityConfig, load_all_entities, load_global_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingestion.loader")


def _connect() -> "snowflake.connector.SnowflakeConnection":
    """Conecta con el rol/warehouse de INGESTA (ROLE_VENTAS_LOADER_FR / WH_VENTAS_INGEST_XS),
    NUNCA con el de dbt: ROLE_VENTAS_TRANSFORMER_FR solo tiene SELECT sobre BRONZE."""
    missing = [v for v in ("SF_ACCOUNT", "SF_USER", "SF_PASSWORD") if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        role=os.environ.get("SF_LOADER_ROLE", "ROLE_VENTAS_LOADER_FR"),
        warehouse=os.environ.get("SF_LOADER_WAREHOUSE", "WH_VENTAS_INGEST_XS"),
    )


def _column_ddl(entity: EntityConfig) -> str:
    return ",\n            ".join(f"{c['name']} {c['type']}" for c in entity.columns)


def _full_table(entity: EntityConfig) -> str:
    return f"{entity.target_database}.{entity.target_schema}.{entity.target_table}"


def _ensure_table(cur, entity: EntityConfig) -> None:
    """CREATE TRANSIENT: BRONZE no necesita Fail-safe, es 100% re-derivable de los CSV."""
    ddl = f"""
        CREATE TRANSIENT TABLE IF NOT EXISTS {_full_table(entity)} (
            {_column_ddl(entity)},
            load_id STRING,
            load_dts TIMESTAMP_TZ,
            record_source STRING,
            source_file STRING,
            source_row NUMBER
        )
        COMMENT = 'BRONZE.LANDING — generada por ingestion/loader.py desde metadata/entities/{entity.entity}.yml';
    """
    cur.execute(ddl)


def _clear_partition(cur, entity: EntityConfig, load_id: str) -> None:
    """Idempotencia: TRUNCATE (full) o DELETE por load_id (append), SIEMPRE seguido
    de COPY con FORCE=TRUE. NUNCA FORCE=FALSE aquí: el historial de carga de
    Snowflake sobrevive al DELETE (solo TRUNCATE lo resetea), así que un
    re-run con FORCE=FALSE saltaría los ficheros ya vistos y cargaría 0 filas
    silenciosamente — justo la trampa que este loader evita a propósito."""
    if entity.load_type == "full":
        cur.execute(f"TRUNCATE TABLE IF EXISTS {_full_table(entity)}")
    elif entity.load_type == "append":
        cur.execute(f"DELETE FROM {_full_table(entity)} WHERE load_id = %s", (load_id,))
    else:
        raise ValueError(f"load_type desconocido en {entity.entity}: {entity.load_type!r}")


def _stage_path(entity: EntityConfig, load_id: str, global_cfg: dict[str, Any]) -> str:
    d = global_cfg["defaults"]
    return f"@{d['bronze_database']}.{d['bronze_schema']}.{d['stage']}/{load_id}/{entity.entity}/"


def _put_file(cur, entity: EntityConfig, load_id: str, global_cfg: dict[str, Any]) -> Path:
    source_root = global_cfg["defaults"]["source_root"].format(load_id=load_id)
    local_path = (REPO_ROOT / entity.source_pattern.format(source_root=source_root)).resolve()
    if not local_path.exists():
        raise FileNotFoundError(f"No existe el fichero de origen: {local_path}")

    stage_path = _stage_path(entity, load_id, global_cfg)
    # file:// con barras normales funciona en Windows y Linux con el conector de Snowflake.
    put_uri = local_path.as_posix()
    cur.execute(f"PUT 'file://{put_uri}' {stage_path} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    return local_path


def _copy_into(cur, entity: EntityConfig, load_id: str, global_cfg: dict[str, Any]) -> tuple[int, str]:
    d = global_cfg["defaults"]
    stage_path = _stage_path(entity, load_id, global_cfg)
    file_format = f"{d['bronze_database']}.{d['bronze_schema']}.{d['file_format']}"

    col_list = ", ".join(c["name"] for c in entity.columns)
    positional = ", ".join(f"${i + 1}" for i in range(len(entity.columns)))

    copy_sql = f"""
        COPY INTO {_full_table(entity)}
            ({col_list}, load_id, load_dts, record_source, source_file, source_row)
        FROM (
            SELECT {positional},
                   %(load_id)s, CURRENT_TIMESTAMP(), %(record_source)s,
                   METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
            FROM {stage_path}
        )
        FILE_FORMAT = (FORMAT_NAME = {file_format})
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE
    """
    cur.execute(copy_sql, {"load_id": load_id, "record_source": f"{d['record_source']}:{load_id}"})
    result = cur.fetchall()
    columns = [c[0] for c in cur.description]
    if not result:
        return 0, ""
    rows_idx = columns.index("rows_loaded")
    file_idx = columns.index("file")
    rows_loaded = sum(row[rows_idx] for row in result)
    file_name = result[0][file_idx]
    return rows_loaded, file_name


def _write_audit(
    cur,
    global_cfg: dict[str, Any],
    *,
    load_id: str,
    entity: str,
    status: str,
    rows_loaded: int,
    file_name: str,
    started_at: datetime,
) -> None:
    d = global_cfg["defaults"]
    audit_table = f"{d['audit_database']}.{d['audit_schema']}.{d['audit_table']}"
    cur.execute(
        f"""
        INSERT INTO {audit_table}
            (load_id, entity, layer, status, rows_loaded, rows_rejected, file_name,
             start_ts, end_ts, airflow_run_id)
        SELECT %(load_id)s, %(entity)s, 'INGEST', %(status)s, %(rows_loaded)s, 0,
               %(file_name)s, %(start_ts)s, CURRENT_TIMESTAMP(), %(run_id)s
        """,
        {
            "load_id": load_id,
            "entity": entity,
            "status": status,
            "rows_loaded": rows_loaded,
            "file_name": file_name,
            "start_ts": started_at,
            "run_id": os.environ.get("AIRFLOW_RUN_ID", "manual"),
        },
    )


def ingest_entity(entity: EntityConfig, load_id: str, global_cfg: dict[str, Any], conn) -> int:
    """Ingiere una entidad para un load_id. Devuelve las filas cargadas. Escribe
    siempre una fila de auditoría (SUCCESS o FAILED) antes de propagar el error."""
    started_at = datetime.now(timezone.utc)
    cur = conn.cursor()
    try:
        _ensure_table(cur, entity)
        _put_file(cur, entity, load_id, global_cfg)
        _clear_partition(cur, entity, load_id)
        rows_loaded, file_name = _copy_into(cur, entity, load_id, global_cfg)
        _write_audit(
            cur, global_cfg, load_id=load_id, entity=entity.entity, status="SUCCESS",
            rows_loaded=rows_loaded, file_name=file_name, started_at=started_at,
        )
        conn.commit()
        log.info("OK   %-10s load_id=%s rows_loaded=%s", entity.entity, load_id, rows_loaded)
        return rows_loaded
    except Exception as exc:
        conn.rollback()
        log.error("FAIL %-10s load_id=%s: %s", entity.entity, load_id, exc)
        try:
            _write_audit(
                cur, global_cfg, load_id=load_id, entity=entity.entity, status="FAILED",
                rows_loaded=0, file_name="", started_at=started_at,
            )
            conn.commit()
        except Exception:
            log.exception("No se pudo escribir la fila de auditoría FAILED de %s", entity.entity)
        raise
    finally:
        cur.close()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--load-id", required=True, help="Identificador de carga, ej. 202512")
    parser.add_argument("--entity", default=None, help="Solo esta entidad (por defecto: todas las habilitadas)")
    args = parser.parse_args(argv)

    global_cfg = load_global_config()
    entities = load_all_entities(only=args.entity)
    if not entities:
        log.error("Ninguna entidad habilitada coincide con --entity=%s", args.entity)
        return 1

    conn = _connect()
    failures = 0
    try:
        for entity in entities:
            try:
                ingest_entity(entity, args.load_id, global_cfg, conn)
            except Exception:
                failures += 1
    finally:
        conn.close()

    if failures:
        log.error("%d entidad(es) fallaron de %d", failures, len(entities))
        return 1
    log.info("Ingesta completa para load_id=%s: %d entidad(es) OK", args.load_id, len(entities))
    return 0


if __name__ == "__main__":
    sys.exit(main())
