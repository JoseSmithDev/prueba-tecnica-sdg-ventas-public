# Ventas — Pipeline metadata-driven sobre Snowflake (TPC-H · Data Vault 2.0 · Kimball · Airflow)

Pipeline end-to-end que ingiere entregas mensuales de CSV (TPC-H), las historiza como
Data Vault 2.0 y las expone en un mart dimensional, orquestado por Airflow. Todo el
pipeline se parametriza con **una sola variable (`load_id`)** y se define en **metadata
declarativa (YAML)**: añadir una entidad nueva no requiere tocar código.

## Arquitectura

```
CSV (data/{load_id}_sample/)
  │  ingestion/loader.py — PUT + COPY INTO, idempotente, con auditoría
  ▼
BRONZE.LANDING          aterrizaje crudo 1:1 + columnas técnicas (load_id, load_dts, source_file)
  │  dbt — vistas raw (filtro por load_id) + staging (hashes hk_/hd_ vía automate_dv)
  ▼
SILVER.STAGING → SILVER.VAULT     Data Vault 2.0: 6 hubs · 6 links · 8 satélites, insert-only
  │  dbt — joins resueltos en build, no en consulta
  ▼
GOLD.MART               estrella Kimball: TB_FCT_ORDER_LINE + dimensiones (SCD2 en cliente)

META.CONTROL            transversal: TB_VENTAS_LOAD_AUDIT, una fila por (entidad, load_id, capa)
```

Cada capa medallion es una **base de datos** (no un esquema): `GRANT ON DATABASE` es la
frontera de aislamiento más simple de razonar y la más difícil de traspasar por error.

## Estructura del repo

```
metadata/       fuente de verdad: config.yml + entities/*.yml + JSON Schema del contrato
ingestion/      loader.py — motor genérico de ingesta (ninguna lógica por entidad)
dbt/            models/{raw,staging,raw_vault,mart} — raw+staging generados desde metadata
scripts/        generate_staging.py — codegen dbt a partir de los YAML
dags/           dag_factory.py — único fichero de DAGs, construido leyendo metadata/
orchestration/  wrappers de Cosmos (un TaskGroup por capa dbt) + task de verificación
snowflake/      00_setup.sql — infraestructura idempotente (databases, roles, warehouses)
tests/          validación del contrato de metadata (pytest, sin Snowflake)
data/           los dos drops de la prueba: 202512_sample/ y 202601_sample/
docs/           guion de slides + propuesta de arquitectura cloud (Azure)
```

## Decisiones de diseño

- **Metadata-driven de verdad**: un YAML por entidad (origen, columnas, full/append,
  clasificación Data Vault, dependencias) validado contra JSON Schema. Lo consumen tres
  piezas independientes: el loader, el codegen de dbt y la fábrica de DAGs.
- **Data Vault 2.0 en Silver**: hubs por clave de negocio, links para relaciones,
  satélites con hashdiff para historizar sin UPDATE/DELETE. `lineitem` y `partsupp` son
  entidades débiles: links puros (con `l_linenumber` como dependent child key), sin hub.
- **automate_dv** genera el SQL de los 20 objetos del vault — el patrón de deduplicación
  (ROW_NUMBER) e historización (LAG sobre hashdiff) se escribe una vez, no veinte.
- **Idempotencia en las dos mitades**: la ingesta hace TRUNCATE (full) o DELETE por
  load_id (append) + COPY `FORCE=TRUE` (el load history de Snowflake sobrevive al DELETE
  y saltaría ficheros en silencio); el vault no inserta filas cuyo hashdiff no cambió.
  Relanzar cualquier carga inserta 0 filas nuevas.
- **RBAC mínimo**: 3 roles funcionales con grants directos — `LOADER_FR` (escribe BRONZE
  y auditoría), `TRANSFORMER_FR` (lee BRONZE, escribe SILVER/GOLD), `ANALYST_FR` (solo
  lee GOLD). Un warehouse XS por carga de trabajo (ingesta/dbt/BI) con auto-suspend 60 s.
- **Transient vs permanent**: BRONZE es transient (re-derivable de los CSV, sin coste de
  Fail-safe); SILVER y GOLD son permanent (`+transient: false`) — el vault es el sistema
  de registro y no se regenera solo.
- **El mart paga los joins una vez**: hechos y dimensiones materializados como tabla
  (incremental el hecho); BI consulta datos ya planos.
- **dbt en venv aislado dentro del contenedor Airflow**: dbt-core reciente y Airflow
  tienen un conflicto real de protobuf; Cosmos apunta al binario del venv
  (`ExecutionConfig`), patrón recomendado por Astronomer.

## Convenciones de nombres

| Objeto | Patrón | Objeto | Patrón |
|---|---|---|---|
| Tablas / vistas | `TB_` / `VW_` | Hub / link / satélite | `hub_` / `lnk_` / `sat_` |
| Warehouses | `WH_*_XS` | Hash key / hashdiff | `hk_` / `hd_` |
| Roles | `ROLE_*_FR` | Columnas mart | `ID_ CO_ DS_ QT_ VL_ DT_ FL_ FK_` |

## Puesta en marcha

```powershell
# 0. Credenciales: copia .env.example a .env y rellena; carga las variables en la sesión.

# 1. Infraestructura Snowflake (una vez, como ACCOUNTADMIN, en Snowsight)
#    -> snowflake/00_setup.sql   (idempotente)

# 2. Ingesta del primer drop
python -m ingestion.loader --load-id 202512

# 3. Transformación + tests (desde dbt/)
dbt deps
dbt build --vars '{load_id: "202512"}'          # 121/121 PASS

# 4. Orquestación completa (Astro CLI + Docker)
astro dev start                                  # UI en localhost:8080
#    Trigger DAG "ventas_monthly" con config {"load_id": "202512"}
```

## La prueba clave: segunda carga sin tocar código

Disparar el mismo DAG con `{"load_id": "202601"}` resuelve el segundo drop completo:
`hub_customer` 20→22, `hub_nation` 10→11 (Portugal), `hub_order` 100→210,
`sat_customer` 20→25 (2 clientes nuevos + 3 con cambios reales — otros 3 diffs del CSV
eran ruido de formato que el tipado `NUMBER(12,2)` filtra correctamente antes del
hashdiff). Relanzar la misma carga: 0 filas nuevas en las 20 tablas del vault.
Verificación reproducible en `dbt/analyses/verify_load_202601.sql`.

## Calidad

- **Contrato**: `python -m pytest` valida cada YAML contra el JSON Schema (hub XOR link,
  prefijos de nombre, tipos de carga) — falla en segundos, sin tocar Snowflake.
- **Datos**: 121 tests dbt (`unique`, `not_null`, `relationships`) intercalados en el
  build: un modelo roto corta el pipeline antes de propagar el error.
- **Operación**: `META.CONTROL.TB_VENTAS_LOAD_AUDIT` traza cada (entidad, load_id, capa)
  con filas cargadas, estado y run de Airflow.

## Documentación

- [`prompts.md`](prompts.md) — prompts de IA utilizados (Rol/Contexto/Petición).
- [`docs/arquitectura_cloud_azure.md`](docs/arquitectura_cloud_azure.md) — propuesta de industrialización en Azure + CI/CD.
- [`docs/guion_slides.md`](docs/guion_slides.md) — guion de la presentación.
- [`docs/demo_nueva_entidad.md`](docs/demo_nueva_entidad.md) — demo en vivo: la segunda carga (`202601`) resuelta solo con un parámetro, para probar el diseño metadata-driven delante del tribunal.
