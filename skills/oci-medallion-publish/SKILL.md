# oci-medallion-publish

Usa esta skill para publicar artefactos en el espejo OCI y dejar todo listo para una futura publicacion real.

## Flujo

1. crear buckets requeridos con `oci-object-storage-mcp`
2. empaquetar aplicaciones con `oci-data-flow-mcp`
3. crear workspace, tasks y pipeline con `oci-data-integration-mcp`
4. registrar ADB, bootstrap y cargas con `oci-autonomous-database-mcp`
5. verificar reportes en `workspace/oci-mirror/<env>/.../reports/`
