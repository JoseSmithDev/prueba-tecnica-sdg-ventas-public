# Registro de prompts de IA — Prueba Técnica SDG

Entregable requerido por el enunciado: listado de los prompts de IA utilizados durante el desarrollo.

**Herramientas utilizadas:**

- **Desarrollo**: todo el código y los artefactos del proyecto se han generado con **Claude Sonnet 5** (vía Claude Code), usando los prompts listados a continuación, uno por fase/tecnología.
- **Consulta de conceptos**: los conceptos que no se tenían claros (terminología de Data Vault 2.0, funcionamiento interno de dbt, componentes de Airflow, etc.) se han buscado con **Gemini Flash** desde Google AI, como apoyo de documentación previa a cada prompt de desarrollo.

Cada prompt sigue la estructura **Rol / Contexto / Petición**. Los prompts 7 y 10 hacen referencia a herramientas (automate_dv, Astro CLI, astronomer-cosmos) propuestas por el propio asistente en los prompts 6 y 9 — el proceso fue incremental, no doce peticiones sueltas.

---

## 1 · Modelado Data Vault 2.0

```
Rol: actúa como arquitecto de datos experto en Data Vault 2.0.
Contexto: tengo 8 CSVs de un sistema de ventas (TPC-H: customer, orders,
lineitem, part, partsupp, supplier, nation, region). Nunca he modelado
Data Vault.
Petición: explícame brevemente las reglas para clasificar hubs, links y
satélites y aplícalas a estas tablas: qué es cada una, con qué business key,
justificado en una frase. Ojo con lineitem y partsupp, que dudo que tengan
identidad propia.
Aplica las preguntas: 
¿Tiene identidad propia e independiente?
¿Es una relación entre dos o más hubs, o una entidad que NO existe sin esa relación?
¿Qué atributos describen a ese hub/link y cambian con el tiempo?
```

## 2 · Setup de Snowflake

```
Rol: actúa como administrador de Snowflake.
Contexto: proyecto nuevo de ventas en una cuenta limpia; el script se ejecuta
una sola vez como ACCOUNTADMIN.
Petición: genera un script SQL idempotente con: 4 databases por capa medallion
(BRONZE/SILVER/GOLD/META) con sus schemas, 3 warehouses XS con auto_suspend=60
separados por carga de trabajo (ingesta/dbt/BI), 3 roles funcionales con
privilegios mínimos y grants directos, un internal stage + file format CSV
(cabecera, campos entrecomillados), y una tabla de auditoría de cargas en META.
Cierra con los SHOW/DESC de verificación.
```

## 3 · Convención de nombres

```
Rol: actúa como responsable de estándares de un equipo de datos.
Contexto: vamos a generar muchos objetos (Snowflake, dbt, metadata) y quiero
consistencia desde el primer día.
Petición: aplica esta nomenclatura a todo lo que generemos: TB_ tablas,
VW_ vistas, WH_ warehouses, roles con sufijo _FR, hub_/lnk_/sat_ en Data Vault
con hk_/hd_ para las claves hash, y columnas del mart con prefijo de tipo
(ID_, CO_, DS_, QT_, VL_, DT_, FL_, FK_). Avísame si algún objeto no encaja.
```

## 4 · Metadata Driven

```
Rol: actúa como ingeniero de datos especializado en pipelines metadata-driven.
Contexto: me piden que añadir una entidad nueva no requiera tocar código —
todo debe declararse en ficheros de configuración.
Petición: propón un YAML por entidad que declare origen (patrón de fichero con
{load_id}), destino en BRONZE, columnas y tipos, carga full o append, su
clasificación data vault (hub/link, business key) y dependencias de
orquestación. Añade un config.yml de defaults y un JSON Schema para validarlos.
Loader, dbt y DAGs tienen que salir de estos ficheros.
```

## 5 · Loader de ingesta

```
Rol: actúa como desarrollador Python con experiencia en ingesta a Snowflake.
Contexto: ya existen los YAMLs de metadata y la infraestructura de Snowflake
del prompt 2.
Petición: genera un loader genérico que lea los YAMLs y, por entidad: cree la
tabla en BRONZE si no existe (TRANSIENT, con columnas técnicas load_id/
load_dts/record_source/fichero), suba el CSV con PUT y haga COPY INTO.
Idempotente: TRUNCATE para full, DELETE por load_id para append, y COPY con
FORCE=TRUE (el load history se me salta ficheros ya cargados). Cada fichero
deja su fila en la tabla de auditoría de META.
```

## 6 · Primeros pasos con dbt

```
Rol: actúa como formador y desarrollador senior de dbt.
Contexto: no he trabajado nunca con dbt; los datos ya están en BRONZE.
Petición: explícame en dos párrafos cómo se estructura un proyecto (models,
materializaciones, vars, tests, packages) y móntame el esqueleto: conexión a
Snowflake por variables de entorno y una capa "raw" de vistas sobre BRONZE
filtrando por la variable load_id. ¿Qué paquete me recomiendas para no
escribir a mano el SQL de hubs, links y satélites?
```

## 7 · Raw vault con automate_dv

```
Rol: actúa como desarrollador experto en automate_dv.
Contexto: en el prompt anterior me propusiste automate_dv; el modelo (6 hubs,
6 links, 8 satélites) ya está decidido en la metadata.
Petición: genera la capa staging con automate_dv.stage() (hashes hk_/hd_
derivados de la metadata) y los 20 modelos del raw vault como incrementales
insert-only. Relanzar el mismo load_id debe insertar 0 filas. Añade tests
unique/not_null en schema.yml y explícame qué hace sat() por debajo — no
quiero usarlo a ciegas.
```

## 8 · Mart dimensional

Contexto: el raw vault ya está cargado en SILVER.VAULT; el consumo será
Power BI.
Petición: genera el mart en GOLD.MART: tabla de hechos a grano línea de pedido
(con medidas derivadas tipo net revenue), dimensión cliente con histórico SCD2
a partir del satélite, proveedor y producto solo en versión actual, y una dim
fecha. Materializadas como tabla, no vista — Power BI no debe pagar los joins
en tiempo de consulta.
```

## 9 · Airflow en local

```
Rol: actúa como ingeniero de plataforma con experiencia en Airflow.
Contexto: nunca he montado Airflow; Docker sí lo conozco; trabajo en Windows.
Petición: ¿qué me recomiendas para levantarlo en local sin pelearme con la
instalación, y cómo monto mis carpetas (metadata, ingestion, dbt, data) dentro
del contenedor para que el scheduler las vea?
```

## 10 · DAG generado desde metadata

```
Rol: actúa como desarrollador senior de Airflow.
Contexto: el Astro CLI que me propusiste ya funciona; toda la definición del
pipeline vive en los YAMLs de metadata.
Petición: genera un único dag_factory.py que lea los YAMLs y construya el DAG
completo — task group de ingesta con una tarea por entidad respetando
depends_on, después las capas dbt en orden usando astronomer-cosmos (me
dijiste que convierte cada modelo dbt en una tarea nativa de Airflow), y una
tarea final de verificación contra la tabla de auditoría. Todo parametrizado
por load_id al disparar el DAG.
```

## 11 · Segunda carga y verificación

```
Rol: actúa como QA de datos.
Contexto: la carga 202512 ya está completa y verificada; voy a cargar 202601
sin tocar código, solo cambiando load_id.
Petición: genera un SQL de verificación antes/después: conteos de hubs y
satélites, qué clientes tienen versión nueva, y comprobar que relanzar la
misma carga no duplica nada. Dime qué números debería esperar si todo ha ido
bien.
```
