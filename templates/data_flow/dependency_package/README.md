# dependency_package

Plantilla base para el `archive.zip` oficial de OCI Data Flow.

## Estructura esperada

- `requirements.txt`
  Dependencias Python externas. No incluir `pyspark` ni `py4j`.
- `packages.txt`
  Paquetes del sistema operativo requeridos por algunas librerias.
- `java/`
  JARs adicionales, por ejemplo Iceberg runtime.

## Uso

1. Copiar esta estructura a `workspace/generated/<project_id>/data_flow/dependencies/<job_name>/`
2. Colocar el jar `iceberg-spark-runtime-3.5_2.12-1.5.2.jar` en `java/`
3. Ejecutar el MCP `oci-data-flow-mcp` con `--command package-dependencies`
