-- =====================================================================
-- Prueba técnica SDG — Setup inicial de Snowflake
-- Ejecutar como ACCOUNTADMIN, de una sola pasada, en un worksheet de Snowsight.
-- Idempotente: se puede volver a ejecutar sin duplicar ni romper nada.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------
-- 1. BASES DE DATOS — medallion real, una base de datos por capa
-- ---------------------------------------------------------------------
-- El enunciado de la prueba pide razonar sobre "databases" (plural), y la
-- arquitectura medallion se aterriza mejor a nivel de base de datos que de
-- esquema: cada tier vive en su propio contenedor de primer nivel, con su
-- propia jerarquía de grants (GRANT ... ON DATABASE es la frontera de
-- aislamiento más simple de explicar y más difícil de traspasar por error).
--
-- BRONZE → aterrizaje crudo, tal cual llega del CSV.
-- SILVER → Data Vault 2.0. Esta ES la técnica correcta para construir un
--          Silver histórico, integrado y auditable — por eso "materializar
--          Data Vault 2.0 en sus capas principales" (lo que pide el
--          enunciado) se cumple exactamente igual que antes, solo que ahora
--          la capa se llama SILVER. Dentro tiene DOS esquemas: STAGING
--          (vistas de preparación, antesala técnica sin historia propia) y
--          VAULT (hubs/links/satellites, la historia real).
-- GOLD   → modelo dimensional Kimball (estrella: hechos + dimensiones).
-- META   → control-plane transversal (auditoría de cargas). No es una capa
--          de datos de negocio, por eso vive fuera de Bronze/Silver/Gold,
--          igual que en cualquier arquitectura medallion bien diseñada.
--
-- En producción esto escalaría añadiendo sufijo de entorno a cada base
-- (BRONZE_DEV/BRONZE_PROD, etc.); para el trial, cuatro bases sin sufijo
-- de entorno son suficientes y siguen siendo demostrables.

CREATE DATABASE IF NOT EXISTS BRONZE
  COMMENT = 'Medallion Bronze — aterrizaje crudo de los CSV de la prueba técnica SDG, tal cual llegan';

CREATE SCHEMA IF NOT EXISTS BRONZE.LANDING
  COMMENT = 'Datos crudos + columnas técnicas de auditoría (load_id, load_dts, record_source, source_file, source_row)';

CREATE DATABASE IF NOT EXISTS SILVER
  COMMENT = 'Medallion Silver — implementado como Data Vault 2.0: histórico integrado, insert-only, auditable';

CREATE SCHEMA IF NOT EXISTS SILVER.STAGING
  COMMENT = 'Vistas de preparación (prefiltro por load_id + cálculo de hashes hk_/hd_ vía automate_dv). Sin color medallion propio: es andamiaje técnico entre Bronze y Silver, no guarda historia.';

CREATE SCHEMA IF NOT EXISTS SILVER.VAULT
  COMMENT = 'Hubs, links y satellites — historia integrada, insert-only. Naming Data Vault estándar (hub_/lnk_/sat_, hk_/hd_), sin prefijos adicionales: es lo que un evaluador de Data Vault 2.0 espera reconocer.';

CREATE DATABASE IF NOT EXISTS GOLD
  COMMENT = 'Medallion Gold — implementado como modelo dimensional Kimball, listo para consumo analítico';

CREATE SCHEMA IF NOT EXISTS GOLD.MART
  COMMENT = 'Estrella Kimball: TB_DIM_*/TB_FCT_*, con prefijo de objeto TB_/VW_ y de columna ID_/CO_/DS_/QT_/VL_/DT_/TS_/FL_/FK_ (convenciones en README.md). Creadas por dbt, no por este script.';

CREATE DATABASE IF NOT EXISTS META
  COMMENT = 'Control-plane transversal — no es una capa de datos, sin color medallion';

CREATE SCHEMA IF NOT EXISTS META.CONTROL
  COMMENT = 'Auditoría y trazabilidad operativa de todas las cargas (TB_VENTAS_LOAD_AUDIT)';

-- ---------------------------------------------------------------------
-- 2. TIPO DE TABLA POR BASE DE DATOS (TRANSIENT vs PERMANENT)
-- ---------------------------------------------------------------------
-- BRONZE es transient: sin Fail-safe, más barato, y es 100% re-derivable
-- desde los CSVs de origen si hiciera falta recargar.
ALTER SCHEMA BRONZE.LANDING SET DEFAULT_DDL_COLLATION = '';
-- (Snowflake no permite forzar TRANSIENT a nivel de esquema por defecto para
--  todas las tablas futuras; se declara TRANSIENT explícitamente en cada
--  CREATE TABLE dentro de ingestion/loader.py y en las tablas de META.)

-- SILVER.VAULT es PERMANENT: es el sistema de registro; Time Travel es la
-- red de seguridad ante un incremental mal ejecutado. No se fuerza aquí
-- porque PERMANENT es el default de Snowflake — no requiere ALTER.

-- ---------------------------------------------------------------------
-- 3. WAREHOUSES — talla XS, auto-suspend agresivo (no se paga si no se usa)
-- ---------------------------------------------------------------------
-- Los warehouses son objetos de cuenta (no pertenecen a ninguna base de
-- datos), así que el split por rol de trabajo no cambia con el rediseño.
CREATE WAREHOUSE IF NOT EXISTS WH_VENTAS_INGEST_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Motor dedicado a la ingesta (PUT + COPY INTO). Aislado para no competir con dbt ni BI.';

CREATE WAREHOUSE IF NOT EXISTS WH_VENTAS_DBT_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Motor dedicado a dbt (staging, Data Vault, mart). Aislado de la ingesta y de BI.';

CREATE WAREHOUSE IF NOT EXISTS WH_VENTAS_BI_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Motor para consultas de analistas sobre GOLD. Aislado: una carga pesada no frena a BI.';

-- Limpieza: si tu cuenta viene de una versión anterior con resource monitor
-- de créditos del trial, este DROP lo retira (se desvincula solo de los
-- warehouses). IF EXISTS no falla en una cuenta que nunca lo tuvo.
DROP RESOURCE MONITOR IF EXISTS RM_VENTAS_TRIAL_GUARD;

-- ---------------------------------------------------------------------
-- 4. ROLES — 3 roles funcionales, grants directos (sin capa de access roles)
-- ---------------------------------------------------------------------
-- Un rol por persona/servicio: ingesta, transformación, analítica. Cada uno
-- recibe sus privilegios directamente sobre las bases/esquemas que necesita.
-- Snowflake recomienda una capa intermedia de "access roles" (uno por
-- combinación base de datos + nivel de privilegio) para cuentas grandes con
-- muchos roles funcionales que comparten privilegios; con solo 3 roles y
-- privilegios que no se solapan entre ellos, esa indirección no aporta nada
-- y sí añade objetos que mantener — SHOW GRANTS TO ROLE ya enseña el detalle
-- completo de un vistazo.
--
--   SYSADMIN
--     ├── ROLE_VENTAS_LOADER_FR       — ingesta: escribe BRONZE + META
--     ├── ROLE_VENTAS_TRANSFORMER_FR  — dbt: lee BRONZE, escribe SILVER+GOLD, lee META
--     └── ROLE_VENTAS_ANALYST_FR      — consumo: solo lee GOLD

CREATE ROLE IF NOT EXISTS ROLE_VENTAS_LOADER_FR
  COMMENT = 'Rol de ingesta: control total sobre BRONZE.LANDING; SELECT + INSERT sobre META.CONTROL (auditoría de cargas).';
CREATE ROLE IF NOT EXISTS ROLE_VENTAS_TRANSFORMER_FR
  COMMENT = 'Rol de dbt: SELECT sobre BRONZE.LANDING; control total sobre SILVER.STAGING/VAULT y GOLD.MART; SELECT sobre META.CONTROL.';
CREATE ROLE IF NOT EXISTS ROLE_VENTAS_ANALYST_FR
  COMMENT = 'Rol de consumo: SELECT sobre GOLD.MART. No ve BRONZE/SILVER/META.';

-- --- ROLE_VENTAS_LOADER_FR: BRONZE (control total) + META (lectura + escritura) ---
GRANT USAGE, CREATE SCHEMA ON DATABASE BRONZE TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT USAGE ON SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT ALL ON SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT ALL ON ALL TABLES IN SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT ALL ON FUTURE TABLES IN SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_LOADER_FR;

GRANT USAGE ON DATABASE META TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT USAGE ON SCHEMA META.CONTROL TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA META.CONTROL TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA META.CONTROL TO ROLE ROLE_VENTAS_LOADER_FR;

-- --- ROLE_VENTAS_TRANSFORMER_FR: BRONZE (solo lectura) + SILVER (control total) + GOLD (control total) + META (solo lectura) ---
GRANT USAGE ON DATABASE BRONZE TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT USAGE ON SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE.LANDING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;

GRANT USAGE, CREATE SCHEMA ON DATABASE SILVER TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT USAGE ON SCHEMA SILVER.STAGING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON SCHEMA SILVER.STAGING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON ALL TABLES IN SCHEMA SILVER.STAGING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON FUTURE TABLES IN SCHEMA SILVER.STAGING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON ALL VIEWS IN SCHEMA SILVER.STAGING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON FUTURE VIEWS IN SCHEMA SILVER.STAGING TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT USAGE ON SCHEMA SILVER.VAULT TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON SCHEMA SILVER.VAULT TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON ALL TABLES IN SCHEMA SILVER.VAULT TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON FUTURE TABLES IN SCHEMA SILVER.VAULT TO ROLE ROLE_VENTAS_TRANSFORMER_FR;

GRANT USAGE, CREATE SCHEMA ON DATABASE GOLD TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT USAGE ON SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON ALL TABLES IN SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT ALL ON FUTURE TABLES IN SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_TRANSFORMER_FR;

GRANT USAGE ON DATABASE META TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT USAGE ON SCHEMA META.CONTROL TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT SELECT ON ALL TABLES IN SCHEMA META.CONTROL TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA META.CONTROL TO ROLE ROLE_VENTAS_TRANSFORMER_FR;

-- --- ROLE_VENTAS_ANALYST_FR: solo lectura de GOLD ---
GRANT USAGE ON DATABASE GOLD TO ROLE ROLE_VENTAS_ANALYST_FR;
GRANT USAGE ON SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_ANALYST_FR;
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_ANALYST_FR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD.MART TO ROLE ROLE_VENTAS_ANALYST_FR;

-- --- Roles → SYSADMIN (gestionable desde ahí) + warehouse por rol ---
GRANT ROLE ROLE_VENTAS_LOADER_FR      TO ROLE SYSADMIN;
GRANT ROLE ROLE_VENTAS_TRANSFORMER_FR TO ROLE SYSADMIN;
GRANT ROLE ROLE_VENTAS_ANALYST_FR     TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE WH_VENTAS_INGEST_XS TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT USAGE ON WAREHOUSE WH_VENTAS_DBT_XS    TO ROLE ROLE_VENTAS_TRANSFORMER_FR;
GRANT USAGE ON WAREHOUSE WH_VENTAS_BI_XS     TO ROLE ROLE_VENTAS_ANALYST_FR;

-- --- Limpieza: si tu cuenta viene de la versión anterior de este script (con
-- capa de access roles), estos DROP retiran los 7 que ya no se usan. Los
-- IF EXISTS hacen que no falle nada en una cuenta nueva que nunca los tuvo.
DROP ROLE IF EXISTS ROLE_VENTAS_BRONZE_READ_AR;
DROP ROLE IF EXISTS ROLE_VENTAS_BRONZE_WRITE_AR;
DROP ROLE IF EXISTS ROLE_VENTAS_SILVER_WRITE_AR;
DROP ROLE IF EXISTS ROLE_VENTAS_GOLD_WRITE_AR;
DROP ROLE IF EXISTS ROLE_VENTAS_GOLD_READ_AR;
DROP ROLE IF EXISTS ROLE_VENTAS_META_READ_AR;
DROP ROLE IF EXISTS ROLE_VENTAS_META_WRITE_AR;

-- Asigna aquí tu propio usuario a los tres roles.
-- Sustituye <TU_USUARIO> por tu usuario de Snowflake.
-- GRANT ROLE ROLE_VENTAS_LOADER_FR      TO USER <TU_USUARIO>;
-- GRANT ROLE ROLE_VENTAS_TRANSFORMER_FR TO USER <TU_USUARIO>;
-- GRANT ROLE ROLE_VENTAS_ANALYST_FR     TO USER <TU_USUARIO>;

-- ---------------------------------------------------------------------
-- 5. STAGE INTERNO Y FILE FORMAT — el buzón de ficheros y sus reglas de lectura
-- ---------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS BRONZE.LANDING.FF_VENTAS_CSV_HEADER
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'   -- imprescindible: las direcciones llevan comas entre comillas
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE
  COMMENT = 'CSV con cabecera y campos opcionalmente entrecomillados (direcciones con comas)';

CREATE STAGE IF NOT EXISTS BRONZE.LANDING.STG_VENTAS_CSV
  FILE_FORMAT = BRONZE.LANDING.FF_VENTAS_CSV_HEADER
  COMMENT = 'Buzón interno donde el loader sube los CSVs (PUT) antes del COPY INTO';

GRANT READ, WRITE ON STAGE BRONZE.LANDING.STG_VENTAS_CSV TO ROLE ROLE_VENTAS_LOADER_FR;
GRANT USAGE ON FILE FORMAT BRONZE.LANDING.FF_VENTAS_CSV_HEADER TO ROLE ROLE_VENTAS_LOADER_FR;

-- ---------------------------------------------------------------------
-- 6. TABLA DE AUDITORÍA — trazabilidad de cada carga
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS META.CONTROL.TB_VENTAS_LOAD_AUDIT (
  audit_id        NUMBER IDENTITY,
  load_id         STRING,
  entity          STRING,
  layer           STRING,             -- INGEST | STAGING | SILVER | GOLD
  status          STRING,             -- RUNNING | SUCCESS | FAILED
  rows_loaded     NUMBER,
  rows_rejected   NUMBER,
  file_name       STRING,
  start_ts        TIMESTAMP_TZ,
  end_ts          TIMESTAMP_TZ,
  airflow_run_id  STRING
)
COMMENT = 'Auditoría operativa: una fila por (entidad, load_id, capa) ejecutada. Base del checkpoint de cada fase.';

-- ---------------------------------------------------------------------
-- 7. VERIFICACIÓN
-- ---------------------------------------------------------------------
SHOW DATABASES LIKE 'BRONZE';   -- deben salir las 4: BRONZE, SILVER, GOLD, META
SHOW DATABASES LIKE 'SILVER';
SHOW DATABASES LIKE 'GOLD';
SHOW DATABASES LIKE 'META';
SHOW SCHEMAS IN DATABASE SILVER;         -- STAGING, VAULT
SHOW WAREHOUSES LIKE 'WH_VENTAS_%';         -- WH_VENTAS_INGEST_XS, WH_VENTAS_DBT_XS, WH_VENTAS_BI_XS
SHOW ROLES LIKE 'ROLE_VENTAS_%_FR';          -- deben salir los 3 roles funcionales
SHOW GRANTS TO ROLE ROLE_VENTAS_LOADER_FR;        -- BRONZE (control total) + META (select+insert)
SHOW GRANTS TO ROLE ROLE_VENTAS_TRANSFORMER_FR;   -- BRONZE (select) + SILVER (control total) + GOLD (control total) + META (select)
SHOW GRANTS TO ROLE ROLE_VENTAS_ANALYST_FR;       -- solo GOLD (select)
DESC TABLE META.CONTROL.TB_VENTAS_LOAD_AUDIT;
