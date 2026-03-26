# oci-data-flow-mcp

MCP operativo para empaquetar dependencias, registrar y ejecutar aplicaciones Data Flow sobre el espejo OCI.

## Modos

- `--runtime local`
  opera sobre el espejo local y genera manifests, dependency archives y runs simulados
- `--runtime oci --oci-mode plan`
  genera el comando OCI CLI real y lo registra en `oci-plans/`
- `--runtime oci --oci-mode apply`
  ejecuta el comando real usando OCI CLI

## Comandos

- empaquetar dependencias con la imagen oficial:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command package-dependencies --application-name bronze-to-silver --dependency-root workspace/generated/sample-project/data_flow/dependencies/bronze-to-silver`
- validar archive:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command validate-archive --application-name bronze-to-silver --dependency-root workspace/generated/sample-project/data_flow/dependencies/bronze-to-silver`
- crear aplicacion:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command create-application --application-name demo-app --source-dir templates/data_flow/minimal_app --dependency-root workspace/generated/sample-project/data_flow/dependencies/demo-app`
- ejecutar aplicacion:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command run-application --application-name demo-app --parameter process_date=2026-03-25`
