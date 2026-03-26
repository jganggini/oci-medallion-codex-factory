# oci-data-catalog-mcp

MCP para gestionar OCI Data Catalog como hub de metadata y lineage del factory medallion.

## Modos

- `--runtime local`
  registra catalogo, assets, conexiones, harvests y eventos OpenLineage en el espejo local
- `--runtime oci --oci-mode plan`
  construye y registra comandos OCI CLI reales
- `--runtime oci --oci-mode apply`
  ejecuta el comando OCI CLI real

## Capacidades

- crear catalogo y private endpoint
- registrar data assets y conexiones para Object Storage, Autonomous o DI
- crear job definitions de harvest y ejecutar jobs
- sincronizar lineage de DI por workspace
- importar eventos OpenLineage desde archivos o desde el outbox del control plane
- consolidar reportes de lineage del factory

## Ejemplos

- crear catalogo:
  `py -3 mcp/servers/oci-data-catalog-mcp/server.py --environment dev --command create-catalog --catalog-name dc-medallion-dev`
- registrar asset de Object Storage:
  `py -3 mcp/servers/oci-data-catalog-mcp/server.py --environment dev --command create-data-asset --asset-name raw-trafico --asset-type-key oracle_object_storage`
- sincronizar lineage de DI:
  `py -3 mcp/servers/oci-data-catalog-mcp/server.py --environment dev --command sync-di-lineage --workspace-name ws-di-medallion-dev --job-key <job_key>`
- importar OpenLineage desde el outbox del control plane:
  `py -3 mcp/servers/oci-data-catalog-mcp/server.py --environment dev --command import-openlineage --from-outbox-file <ruta>`
