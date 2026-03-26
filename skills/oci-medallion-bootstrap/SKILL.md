# oci-medallion-bootstrap

Usa esta skill para preparar la base de un proyecto medallion nuevo.

## Flujo

1. validar el scaffold con `py -3 scripts/validate_factory.py --repo-root .`
2. inicializar el workspace con `setup-dev.ps1` o `setup-dev.sh`
3. confirmar `project.medallion.yaml`, especialmente `existing_buckets`, `source_assets`, `control_plane`, `lineage` y `reprocess`
4. revisar `workspace/oci-mirror/<env>/`
5. crear o sincronizar buckets base con el MCP de Object Storage sin asumir capas por existencia de datos
6. si el proyecto usa ADW, registrar `create-adb-definition`, `bootstrap-control-plane` y preparar el bootstrap de usuario/tablas con `create-database-user`, `bootstrap-schema` y `apply-sql`
7. si el proyecto usa Data Catalog, preparar private endpoint, assets y harvests
8. si Terraform o el recurso OCI no estan claros, consultar `oci-terraform-fallback` antes de cambiar `infra/` o un MCP
