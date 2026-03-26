# oci-medallion-publish

Usa esta skill para publicar artefactos en el espejo OCI y dejar todo listo para una futura publicacion real.

## Flujo

1. crear buckets requeridos con `oci-object-storage-mcp`
2. sincronizar buckets existentes si la carga llega por fuera del factory
3. empaquetar aplicaciones con `oci-data-flow-mcp`
4. crear workspace, proyectos, tasks y pipeline con `oci-data-integration-mcp`
5. registrar ADB, bootstrapear `MDL_CTL`, crear usuario, aplicar DDL y registrar cargas con `oci-autonomous-database-mcp`
6. registrar Data Catalog, assets, harvests e importar lineage cuando aplique
7. ejecutar QA contractual con `oci-data-quality-mcp`
8. verificar reportes en `workspace/oci-mirror/<env>/.../reports/`, `quality/`, `data_catalog/` y `autonomous_database/<adb>/control_plane/`
9. si algun recurso o plan Terraform no coincide con el servicio OCI, consultar `oci-terraform-fallback` antes de modificar el despliegue
