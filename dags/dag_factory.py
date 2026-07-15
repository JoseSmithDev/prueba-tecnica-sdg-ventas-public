"""Único fichero de dags/: genera un DAG por cada `dag_group` de
metadata/config.yml, leyendo metadata/entities/*.yml en tiempo de parseo.

Este fichero NO menciona ninguna tabla, entidad ni load_id concreto — todo
sale de la metadata. Añadir una entidad = un YAML nuevo en metadata/entities/;
añadir un dag_group = una entrada nueva en metadata/config.yml. Cero cambios
de Python en ninguno de los dos casos.

Estructura por DAG: TaskGroup "ingest" (una task por entidad habilitada,
cableada por el grafo depends_on de la metadata) >> un DbtTaskGroup de Cosmos
por capa dbt (raw -> staging -> raw_vault -> mart) >> task "verify" (falla el
run si META.CONTROL.TB_VENTAS_LOAD_AUDIT no tiene SUCCESS para todas las
entidades de este run).
"""
from __future__ import annotations

import os

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ingestion.loader import _connect as _connect_loader
from ingestion.loader import ingest_entity
from ingestion.metadata import load_all_entities, load_global_config
from orchestration.dbt_layers import dbt_layer_group
from orchestration.verify import check_load_audit


def _run_ingest(entity: str, load_id: str, run_id: str) -> None:
    """Wrapper picklable para el PythonOperator: resuelve la EntityConfig desde
    su nombre (Airflow solo templa strings/kwargs) y delega en el motor
    genérico de ingestion/loader.py — la misma función que se usa desde la
    terminal en la Fase 3."""
    os.environ["AIRFLOW_RUN_ID"] = run_id
    global_cfg = load_global_config()
    entity_cfg = load_all_entities(only=entity)[0]
    conn = _connect_loader()
    try:
        ingest_entity(entity_cfg, load_id, global_cfg, conn)
    finally:
        conn.close()


GLOBAL_CFG = load_global_config()
ALL_ENTITIES = load_all_entities()

for group_name, group_cfg in GLOBAL_CFG["dag_groups"].items():
    group_entities = [e for e in ALL_ENTITIES if e.dag_group == group_name]

    with DAG(
        dag_id=group_name,
        schedule=group_cfg["schedule"],
        catchup=False,
        params={"load_id": Param(group_cfg["default_load_id"], type="string")},
        tags=["metadata-generated"],
    ) as dag:

        with TaskGroup("ingest") as ingest_group:
            tasks = {
                entity_cfg.entity: PythonOperator(
                    task_id=f"load_{entity_cfg.entity}",
                    python_callable=_run_ingest,
                    op_kwargs={
                        "entity": entity_cfg.entity,
                        "load_id": "{{ params.load_id }}",
                        "run_id": "{{ run_id }}",
                    },
                )
                for entity_cfg in group_entities
            }
            for entity_cfg in group_entities:
                for dep in entity_cfg.depends_on:
                    if dep in tasks:
                        tasks[dep] >> tasks[entity_cfg.entity]

        prev = ingest_group
        for layer in group_cfg["dbt_layers"]:
            layer_group = dbt_layer_group(layer)
            prev >> layer_group
            prev = layer_group

        if group_cfg.get("run_tests", True):
            verify_task = PythonOperator(
                task_id="verify",
                python_callable=check_load_audit,
                op_kwargs={"load_id": "{{ params.load_id }}", "run_id": "{{ run_id }}"},
            )
            prev >> verify_task

    globals()[group_name] = dag
