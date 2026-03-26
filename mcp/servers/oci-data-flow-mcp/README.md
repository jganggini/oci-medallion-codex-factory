# oci-data-flow-mcp

MCP operativo en modo local para empaquetar, registrar y ejecutar aplicaciones Data Flow sobre el espejo OCI.

## Modos

- `--runtime local`
  opera sobre el espejo local y genera `archive.zip`, manifests y runs simulados
- `--runtime oci --oci-mode plan`
  genera el comando OCI CLI real y lo registra en `oci-plans/`
- `--runtime oci --oci-mode apply`
  ejecuta el comando real usando OCI CLI

## Comandos

- crear aplicacion:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command create-application --application-name demo-app --source-dir templates/data_flow/minimal_app`
- ejecutar aplicacion:
  `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command run-application --application-name demo-app --parameter process_date=2026-03-25`
