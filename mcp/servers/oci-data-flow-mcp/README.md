# oci-data-flow-mcp

MCP operativo para empaquetar dependencias, registrar, actualizar y ejecutar aplicaciones Data Flow sobre el espejo OCI.

## Modos

- `--runtime local`
  genera `application.manifest.json`, `application.json` o `archive.zip` segun la entrada, dependency archives y runs simulados
- `--runtime oci --oci-mode plan`
  registra el comando OCI CLI real en `oci-plans/`
- `--runtime oci --oci-mode apply`
  ejecuta el comando real usando OCI CLI

## Capacidades

- empaquetar dependencias con la imagen oficial:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command package-dependencies --application-name bronze-to-silver --dependency-root workspace/generated/sample-project/data_flow/dependencies/bronze-to-silver`
- validar archive:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command validate-archive --application-name bronze-to-silver --dependency-root workspace/generated/sample-project/data_flow/dependencies/bronze-to-silver`
- soporta `create-application`, `update-application` y `run-application`
- acepta `--from-json-file` y `--archive-source-file` aun cuando los archivos vivan fuera del repo
- soporta `--force`, multiples `--wait-for-state`, `--max-wait-seconds` y `--wait-interval-seconds`
- soporta `--driver-shape-config-json` / `--executor-shape-config-json`
- soporta atajos numericos para Flex shapes:
  `--driver-shape-ocpus`, `--driver-shape-memory-gbs`, `--executor-shape-ocpus`, `--executor-shape-memory-gbs`

## Ejemplos

- crear desde codigo fuente local:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command create-application --application-name demo-app --source-dir templates/data_flow/minimal_app --dependency-root workspace/generated/sample-project/data_flow/dependencies/demo-app`
- crear desde `application.json` externo:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --runtime oci --oci-mode plan --command create-application --application-name bronze-json-app --from-json-file D:\ruta\application.json --compartment-id ocid1.compartment... --wait-for-state ACTIVE`
- actualizar una aplicacion existente:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --runtime oci --oci-mode plan --command update-application --application-name bronze-json-app --application-id ocid1.dataflowapplication... --from-json-file D:\ruta\application.json --force --wait-for-state ACTIVE`
- ejecutar una aplicacion:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command run-application --application-name demo-app --parameter process_date=2026-03-25`
