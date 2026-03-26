# Data Flow Local Runtime

## Objetivo

Ejecutar pruebas locales de jobs Spark/Data Flow sin instalar Python, Java o Spark de forma global.

## Imagen

- base: `python:3.11-slim-bookworm`
- Java: OpenJDK 17
- Python packages:
  - `pyspark==3.5.0`
  - `oracledb`
  - `oci`

## Servicio Docker

`dataflow-local`

## Scripts principales

- `scripts/run_dataflow_local.ps1`
- `scripts/run_dataflow_local.sh`

## Ejemplo

```powershell
.\scripts\run_dataflow_local.ps1 `
  -BuildImage `
  -JobPath templates\data_flow\minimal_app\main.py `
  -ProcessDate 20260325 `
  -ProjectId sample-project `
  -Environment dev
```

Con jar de Iceberg:

```powershell
.\scripts\run_dataflow_local.ps1 `
  -JobPath path\to\job.py `
  -ProcessDate 20260325 `
  -JarPath workspace\generated\sample-project\data_flow\dependencies\bronze-to-silver\java\iceberg-spark-runtime-3.5_2.12-1.5.2.jar `
  -SparkConf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

## Montajes

- repo host -> `/workspace`
- `.local/oci` -> `/mnt/oci`
- wallets -> `/mnt/wallets`

## Recomendacion

Usar este runtime para validar jobs y usar `dependency-packager` para construir el `archive.zip` que luego se publica a OCI Data Flow.
