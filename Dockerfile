FROM quay.io/astronomer/astro-runtime:13.6.0

# dbt vive en su propio venv, NO en el venv de Airflow: dbt-core reciente
# exige protobuf>=6.0, y el opentelemetry-proto que trae Airflow exige
# protobuf<6.0 (conflicto real, verificado contra varias versiones de
# astro-runtime y de dbt-core/dbt-adapters — ninguna combinación coexiste en
# un mismo entorno ahora mismo). Se aísla dbt en su venv y se apunta Cosmos a
# ese binario vía ExecutionConfig(dbt_executable_path=...) en
# orchestration/dbt_layers.py — el patrón que recomienda Astronomer para
# justo este tipo de choque de dependencias.
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.11.6 dbt-snowflake==1.11.6

# ingestion/, orchestration/, dbt/ y metadata/ se montan en /usr/local/airflow
# vía docker-compose.override.yml (ver Fase 5). Astro Runtime solo añade
# include/ al PYTHONPATH por defecto; esto permite `from ingestion.loader
# import ...` / `from orchestration.dbt_layers import ...` sin duplicar el
# código dentro de include/.
ENV PYTHONPATH="/usr/local/airflow:${PYTHONPATH}"
