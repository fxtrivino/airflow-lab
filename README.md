# Airflow Lab — Orquestación de pipelines de datos

**Proyecto de Data Engineering con Apache Airflow, Docker, PostgreSQL, Apache Spark, Delta Lake, Snowflake y dbt.**

> **English summary:** A hands-on data engineering orchestration lab integrating Python ETL, Spark/Delta Lake and Snowflake/dbt pipelines with Apache Airflow and Docker Compose.

## Objetivo

Centralizar la **orquestación, ejecución y supervisión** de diferentes pipelines de datos. El repositorio contiene los DAGs, la configuración de contenedores y servicios *runner*; parte de la lógica ETL y de los modelos dbt reside en repositorios independientes.

Este es un **entorno de aprendizaje e integración local**, no una plataforma productiva lista para desplegarse.

## Arquitectura

```text
                         Apache Airflow 3.1.5
                    (scheduler / DAG processor / API)
                                  |
                +-----------------+-----------------+
                |                 |                 |
          Python ETL         Spark ETL         dbt runner
                |                 |                 |
           PostgreSQL       Spark + Delta     Snowflake + dbt
                |
        Datos y auditoría ETL

       PostgreSQL 16 adicional: metadatos internos de Airflow
```

**Importante:** el PostgreSQL del servicio `postgres` en este Compose almacena los **metadatos de Airflow**. Los datos operativos de los pipelines pueden depender de otra instancia de PostgreSQL, definida por el proyecto ETL correspondiente.

### Servicios Docker Compose

| Servicio | Función |
| --- | --- |
| `postgres` | Base de metadatos de Airflow (PostgreSQL 16) |
| `airflow-init` | Inicialización de Airflow |
| `airflow-apiserver` | API e interfaz de Airflow, publicada en `localhost:8085` |
| `airflow-scheduler` | Planificación de ejecuciones |
| `airflow-dag-processor` | Procesamiento de definiciones DAG |
| `spark-etl` | Servicio para ejecutar el proyecto Spark externo |
| `dbt-runner` | Servicio para ejecutar el proyecto Snowflake/dbt externo |

## DAGs incluidos

Los siguientes archivos están presentes en `dags/`. Su lógica y dependencias específicas deben consultarse en el código fuente.

| Archivo | Área |
| --- | --- |
| `customers_incremental_etl.py` | ETL incremental de clientes con Spark |
| `orders_etl_pipeline_dag.py` | Pipeline ETL de órdenes |
| `prepare_etl_partitions_dag.py` | Preparación de particiones ETL |
| `retry_etl_dag.py` | Reintentos de procesos ETL |
| `etl_batch_monitor.py` | Supervisión de lotes ETL |
| `snowflake_customers_pipeline.py` | Orquestación de clientes en Snowflake |
| `snowflake_orders_pipeline.py` | Orquestación de órdenes en Snowflake |
| `snowflake_payments_pipeline.py` | Orquestación de pagos en Snowflake |

El directorio `archived_dags/` contiene una versión archivada de un DAG de Snowflake/dbt; no forma parte de los DAGs activos montados desde `dags/`.

## Dependencias entre repositorios

La configuración actual usa **rutas relativas de montaje**, por lo que se espera la siguiente disposición local:

```text
workspace/
├── airflow-lab/
├── misproyectospython/
│   └── python-data-engineering-lab/
├── pyspark-delta-incremental-etl/
└── snowflake-dbt-data-engineering/
    ├── snowflake_data_platform/
    └── data/
```

- **Python Data Engineering Lab:** código ETL montado en `/opt/airflow/etl`.
- **PySpark Delta Incremental ETL:** código montado en `/opt/spark-etl`.
- **Snowflake dbt Data Engineering:** proyecto dbt y datasets montados en el contenedor `dbt-runner`.

> **Limitación de reproducibilidad:** este repositorio por sí solo no incluye el código de los otros proyectos. Además, `airflow-scheduler` se conecta a una red Docker externa llamada `python-data-engineering-lab_default`, que debe existir antes de iniciar el stack. Los repositorios externos, sus instrucciones de configuración y sus dependencias deben prepararse por separado.

## Requisitos

- Docker Engine / Docker Desktop y Docker Compose.
- Copias locales de los repositorios externos en las rutas indicadas.
- Un archivo `.env` local con la configuración requerida.
- Para los DAGs de Snowflake: una cuenta configurada, permisos apropiados y una clave privada RSA disponible localmente.
- Para ejecutar todos los DAGs: las conexiones y servicios de datos definidos por cada proyecto externo.

## Configuración local

### 1. Variables de entorno

Crea tu archivo local a partir de la plantilla:

```bash
cp .env.example .env
```

Configura los valores requeridos en `.env`, incluidos los de PostgreSQL, el administrador de Airflow y `DBT_PRIVATE_KEY_PASSPHRASE`.

**No publiques `.env`, claves privadas ni contraseñas reales.** Las variables de PostgreSQL y Airflow deben coincidir con las definidas en `docker-compose.yaml`. Si ya existe un volumen PostgreSQL inicializado, modificar `POSTGRES_PASSWORD` en `.env` no cambia la contraseña almacenada en la base de datos.

### 2. Perfil dbt y clave RSA

```bash
cp dbt/profiles/profiles.yml.example dbt/profiles/profiles.yml
```

Edita `profiles.yml` con los identificadores de tu entorno Snowflake. La configuración usa una clave RSA montada desde:

```text
${HOME}/.snowflake/keys/dbt_rsa_key.p8
```

La contraseña de la clave se obtiene desde `DBT_PRIVATE_KEY_PASSPHRASE`. Ni el archivo de clave ni el perfil real deben versionarse.

### 3. Red Docker externa

El Compose actual requiere una red llamada `python-data-engineering-lab_default`. Comprueba que exista:

```bash
docker network inspect python-data-engineering-lab_default
```

Si no existe, primero configura el proyecto externo que la crea o revisa la configuración de redes. No crees una red con ese nombre sin comprobar que permite la conectividad requerida por tus pipelines.

### 4. Validación e inicio

```bash
docker compose config --quiet
docker compose config --services
docker compose up -d --build
```

Interfaz de Airflow: **http://localhost:8085**.

Comprobaciones básicas:

```bash
docker compose ps
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 airflow-dag-processor
```

> Estos comandos permiten levantar y revisar los servicios, pero **no garantizan** que todos los DAGs funcionen: cada uno puede requerir conexiones, credenciales, tablas, datasets y servicios externos adicionales.

## Decisiones técnicas

- **Separación entre orquestación y transformación:** Airflow define el flujo; los proyectos externos implementan las operaciones de datos.
- **Contenedores especializados:** Spark y dbt tienen runners independientes, evitando instalar todas las dependencias en el contenedor de Airflow.
- **Configuración sensible fuera de Git:** `.env`, `profiles.yml` y la clave RSA se mantienen locales.
- **Múltiples escenarios de integración:** se practican diferentes herramientas sin presentar el repositorio como una única solución de negocio.
