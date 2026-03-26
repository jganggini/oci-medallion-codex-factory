# oci-medallion-publish

Usa esta skill para publicar artefactos en el espejo OCI y dejar todo listo para una futura publicacion real.

## Flujo

1. crear buckets requeridos con `oci-object-storage-mcp`
2. empaquetar aplicaciones con `oci-data-flow-mcp`
3. crear workspace, proyectos, tasks y pipeline con `oci-data-integration-mcp`
4. registrar ADB, crear usuario, aplicar DDL y registrar cargas con `oci-autonomous-database-mcp`
5. ejecutar QA contractual con `oci-data-quality-mcp`
6. verificar reportes en `workspace/oci-mirror/<env>/.../reports/` y `quality/`
7. si algun recurso o plan Terraform no coincide con el servicio OCI, consultar `oci-terraform-fallback` antes de modificar el despliegue
