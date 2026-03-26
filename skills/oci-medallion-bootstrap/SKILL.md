# oci-medallion-bootstrap

Usa esta skill para preparar la base de un proyecto medallion nuevo.

## Flujo

1. validar el scaffold con `py -3 scripts/validate_factory.py --repo-root .`
2. pedir la ruta exacta del archivo OCI `config`
3. pedir la ruta exacta de la llave privada `.pem`
4. si esos archivos aun no estan en `.local/oci/`, ejecutar `py -3 scripts/stage_local_assets.py` para copiarlos a `.local/oci/config` y `.local/oci/key.pem`
5. si el proyecto usa ADW, pedir la ruta exacta del wallet y stagearlo en `.local/autonomous/wallets/<env>/<adb_name>/`
6. bloquear bootstrap si esas rutas no estan confirmadas o stageadas
7. inicializar el workspace con `setup-dev.ps1` o `setup-dev.sh`
8. confirmar `project.medallion.yaml`, especialmente `existing_buckets`, `source_assets`, `control_plane`, `lineage` y `reprocess`
9. revisar `workspace/oci-mirror/<env>/`
10. crear o sincronizar buckets base con el MCP de Object Storage sin asumir capas por existencia de datos
11. si el proyecto usa ADW, registrar `create-adb-definition`, `bootstrap-control-plane` y preparar el bootstrap de usuario/tablas con `create-database-user`, `bootstrap-schema` y `apply-sql`
12. si el proyecto usa Data Catalog, preparar private endpoint, assets y harvests
13. si Terraform o el recurso OCI no estan claros, consultar `oci-terraform-fallback` antes de cambiar `infra/` o un MCP
