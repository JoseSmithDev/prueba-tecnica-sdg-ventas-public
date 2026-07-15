# Demo en vivo: segunda carga (`202601`)

La demo prueba en vivo que una **carga incremental** (mes nuevo) se resuelve sola,
disparando el mismo DAG y cambiando solo un parámetro (`load_id`) — sin tocar ni un
fichero de código. Es la afirmación que más pesa en la evaluación: *"el contrato es la
metadata, no el código"*, aplicada aquí a la dimensión temporal en vez de a una entidad
nueva.

---

## Preparación técnica (hacer ANTES de entrar en la reunión)

### Paso 0 — Abrir PowerShell en la raíz del repo

```powershell
cd "C:\Users\jquin\Desktop\Prueba Técnica"
```

### Paso 1 — Cargar las credenciales de Snowflake desde `.env`

El proyecto **no** carga `.env` automáticamente (no hay `python-dotenv` instalado a
propósito, para no añadir una dependencia solo para esto). Cada sesión de terminal nueva
hay que volcar el fichero a variables de entorno del proceso actual — así nunca escribes
la contraseña a mano ni queda en el historial de comandos:

```powershell
Get-Content ".env" | ForEach-Object {
    if ($_ -match '^\s*([A-Z_]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
    }
}
```

Esto deja disponibles `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`,
`SF_DATABASE`, `SF_LOADER_ROLE`, `SF_LOADER_WAREHOUSE` — las mismas que lee
`dbt/profiles.yml` (vía `env_var(...)`) y `ingestion/loader.py` (vía `os.environ`).
**Hay que repetir este paso cada vez que abras una terminal nueva** — las variables no
persisten entre sesiones.

### Paso 2 — Resetear Snowflake al estado base (solo `202512`)

Si en algún ensayo anterior se disparó ya `202601`, Snowflake necesita volver al punto
de partida antes de la reunión real. Son dos comandos:

```powershell
# 2a. Re-ingesta 202512: restaura en Bronze las entidades "full" que la
#     última carga (202601) truncó (TRUNCATE + COPY en cada ingesta full).
& ".venv\Scripts\python.exe" -m ingestion.loader --load-id 202512
```

```powershell
# 2b. Full-refresh de dbt: reconstruye Silver/Gold desde cero usando SOLO
#     lo que las vistas raw ven para load_id=202512 (el filtro está en
#     dbt/models/raw/vw_raw_*.sql). Esto "borra" 202601 de Silver/Gold sin
#     necesidad de un DELETE manual.
Set-Location "dbt"
& "..\.venv\Scripts\dbt.exe" build --full-refresh --vars '{load_id: "202512"}'
Set-Location ..
```

Salida esperada: **`Done. PASS=121 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=121`** (8 entidades).

### Paso 3 — Verificar en Snowsight que quedó en el estado base

```sql
SELECT COUNT(*) FROM SILVER.VAULT.HUB_CUSTOMER;   -- 20
SELECT COUNT(*) FROM SILVER.VAULT.HUB_ORDER;      -- 100
SELECT COUNT(*) FROM SILVER.VAULT.HUB_NATION;     -- 10
```

Si los tres números coinciden, Snowflake está exactamente en el estado que espera la
demo. Si `HUB_ORDER` ya marca 210 en vez de 100, repite el Paso 2 — la demo insertaría 0
filas nuevas y perdería el efecto en directo.

> **Chuleta rápida para el día de la reunión:** Paso 1 (cargar `.env`) → Paso 2a
> (`loader --load-id 202512`) → Paso 2b (`dbt build --full-refresh`) → Paso 3
> (verificar 20/100/10) → arrancar Airflow (`astro dev start`) → empezar la demo.

---

## Guion de la demo (3-4 min)

### 1. Contexto (20 s)
> "Snowflake tiene ahora mismo un solo mes cargado, diciembre. Voy a disparar el mismo
> DAG cambiando solo un parámetro — `load_id` — y vais a ver cómo entra el mes de enero
> sin tocar ni un fichero."

### 2. Dispara el DAG (1 min)
Desde la UI de Airflow (`localhost:8080`), DAG `ventas_monthly` → **Trigger DAG w/ config**
→ `{"load_id": "202601"}`. Espera a verlo en verde de punta a punta (ingesta → dbt →
verify).

*(Alternativa sin Airflow, si hace falta lanzarlo a mano):*
```powershell
python -m ingestion.loader --load-id 202601
cd dbt
dbt build --vars '{load_id: "202601"}'
```

### 3. Enseña los números cambiando solos (1-2 min)
```sql
SELECT COUNT(*) FROM SILVER.VAULT.HUB_CUSTOMER;   -- 20 → 22
SELECT COUNT(*) FROM SILVER.VAULT.HUB_NATION;     -- 10 → 11 (Portugal, nueva)
SELECT COUNT(*) FROM SILVER.VAULT.HUB_ORDER;      -- 100 → 210 (+110, sin solape)
SELECT COUNT(*) FROM SILVER.VAULT.SAT_CUSTOMER;   -- 20 → 25 (+5: 2 nuevos + 3 con cambio real)
```
**El matiz que gana puntos** (si preguntan por qué no +8 en `sat_customer`): el CSV de
clientes tiene 6 filas con diferencias de texto, pero 3 son solo un cero decimal de más
(`2091.20` vs `2091.2`) — al tipar `NUMBER(12,2)` en Bronze, el hashdiff correctamente
NO lo trata como cambio. Solo 3 clientes cambiaron de verdad.

### 4. Prueba de idempotencia (30 s) — opcional si sobra tiempo
Vuelve a disparar el DAG con el mismo `{"load_id": "202601"}`: mismos contadores, **0
filas nuevas**. Referencia completa de verificación: `dbt/analyses/verify_load_202601.sql`.

### 5. Cierre (20 s)
> "Este mecanismo es el mismo, sin excepción, tanto si el mes trae 110 pedidos nuevos
> como si trae 10.000: el contrato es el parámetro `load_id`, nunca una línea de código
> tocada para admitir un mes distinto."

## Si algo falla

- **La demo (202601) inserta 0 filas nuevas**: síntoma de que Snowflake ya tenía 202601
  cargado desde antes — repite el reset del principio de este documento.
- **Airflow no arranca o tarda en refrescar el DAG**: espera un parseo completo (barra de
  progreso en la UI) o `astro dev restart` si hay prisa.
- **Quieres revertir la demo por completo** (dejar Snowflake en el estado base de nuevo):
  repite el reset del principio (`loader --load-id 202512` + `dbt build --full-refresh`).
