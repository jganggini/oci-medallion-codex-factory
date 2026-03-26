# oci-object-storage-mcp

MCP operativo para administrar buckets por capa, sincronizar manifests de buckets existentes y dejar trazabilidad en el espejo OCI.

## Modos

- `--runtime local`
  opera sobre el espejo local
- `--runtime oci --oci-mode plan`
  registra el comando OCI CLI real sin ejecutarlo
- `--runtime oci --oci-mode apply`
  ejecuta el comando OCI CLI real

## Comandos

- crear bucket:
  `py -3 mcp/servers/oci-object-storage-mcp/server.py --environment dev --command create-bucket --bucket-name bucket-raw`
- cargar objeto:
  `py -3 mcp/servers/oci-object-storage-mcp/server.py --environment dev --command upload-object --bucket-name bucket-raw --source-file <ruta>`
- sincronizar un bucket existente sin asumir que creo todas las capas:
  `py -3 mcp/servers/oci-object-storage-mcp/server.py --environment dev --command sync-bucket-manifest --bucket-name bucket-fuente --layer landing_external --managed-by-factory false --ingestion-outside-flow true`
