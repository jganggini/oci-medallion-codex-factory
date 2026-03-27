# Data Flow Dependency Packager

## Objetivo

Usar la imagen oficial de OCI Data Flow Dependency Packager para construir el `archive.zip` de dependencias que luego se publica con la aplicacion.

## Flujo recomendado

1. crear una carpeta por job en `workspace/generated/<project_id>/data_flow/dependencies/<job_name>/`
2. copiar la plantilla de `templates/data_flow/dependency_package/`
3. agregar `requirements.txt`, `packages.txt` y JARs en `java/`
4. colocar el jar `iceberg-spark-runtime-3.5_2.12-1.5.2.jar` dentro de `java/` cuando el job use Iceberg
5. ejecutar `package-dependencies`
6. publicar `archive.zip` al bucket y referenciarlo con `--archive-uri`

## Comando

`powershell -ExecutionPolicy Bypass -File .\scripts\docker_repo_python.ps1 mcp/servers/oci-data-flow-mcp/server.py --repo-root . --environment dev --command package-dependencies --application-name bronze-to-silver --dependency-root workspace/generated/sample-project/data_flow/dependencies/bronze-to-silver`

## Nota

El script soporta la imagen legacy `phx.ocir.io/oracle/dataflow/dependency-packager:latest` con `--use-legacy-packager-image`, pero por defecto queda listo para usar una imagen oficial por arquitectura cuando corresponda.

Si trabajas en Windows, el wrapper tambien normaliza rutas del repo para evitar fallos por `\` en argumentos como `--dependency-root`.
