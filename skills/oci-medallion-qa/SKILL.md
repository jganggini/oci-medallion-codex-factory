# oci-medallion-qa

Usa esta skill para ejecutar QA de datos migrados en una arquitectura medallion sobre buckets y Autonomous Database, especialmente cuando el proyecto tenga contratos en `workspace/migration-input/<project_id>/quality/contracts/`.

## Flujo

1. ubicar el contrato del dataset en `quality/contracts/`
2. perfilar el extracto o export Gold con `oci-data-quality-mcp --command profile-bucket-data`
3. ejecutar `oci-data-quality-mcp --command run-contract` en modo local para validar bucket y artefactos ADB
4. antes del cutover, ejecutar el mismo contrato con `--runtime oci --oci-mode apply` para correr los SQL QA en Autonomous
5. cerrar con `oci-data-quality-mcp --command gate-migration` y revisar `workspace/oci-mirror/<env>/.../quality/`
