"""Envoltorio Cosmos: un DbtTaskGroup por capa dbt (raw/staging/raw_vault/mart).

No se menciona ningún load_id/entidad/modelo aquí — la capa se selecciona
por `tag:<layer>` (los tags ya existen en dbt/dbt_project.yml) y el load_id
viaja como var templada desde el parámetro del DAG. Añadir una capa nueva
solo requiere añadirla a `metadata/config.yml -> dag_groups.*.dbt_layers`.

dbt vive en un venv aislado del de Airflow (ver Dockerfile): dbt-core
reciente exige protobuf>=6.0 y Airflow trae opentelemetry-proto que exige
protobuf<6.0 — conflicto real del ecosistema, sin combinación posible ahora
mismo si ambos comparten site-packages. `dbt_executable_path` le dice a
Cosmos (tanto para parsear como para ejecutar) que invoque el dbt de ese
venv en vez del que buscaría por defecto en el PATH de Airflow.

Nota de rendimiento (documentada, no un descuido): sin `manifest.json`
precompilado, Cosmos usa LoadMode.AUTOMATIC -> cae a `dbt ls`, que además
necesita una conexión real a Snowflake porque los modelos de automate_dv
introspeccionan columnas en tiempo de compilación (ver vw_stg_*.sql). Eso
se repite en cada parseo del scheduler (~30s). Para un demo de este tamaño
(41 modelos) es asumible; en producción se generaría `manifest.json` en un
paso de CI y se usaría LoadMode.DBT_MANIFEST.
"""
from __future__ import annotations

from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode

DBT_PROJECT_DIR = Path("/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

PROFILE_CONFIG = ProfileConfig(
    profile_name="ventas",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

# InvocationMode.SUBPROCESS (no el DBT_RUNNER por defecto de Cosmos): dbt no
# es importable como librería Python dentro del venv de Airflow (vive en su
# propio venv aislado, ver Dockerfile) — Cosmos debe invocarlo por
# subprocess, no intentar cargarlo en proceso.
EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    invocation_mode=InvocationMode.SUBPROCESS,
)


def dbt_layer_group(layer: str) -> DbtTaskGroup:
    """DbtTaskGroup con un task por modelo dbt de la capa `layer`, encadenable
    con `>>` como cualquier TaskGroup normal de Airflow."""
    return DbtTaskGroup(
        group_id=f"dbt_{layer}",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=[f"tag:{layer}"],
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        operator_args={"vars": '{"load_id": "{{ params.load_id }}"}'},
    )
