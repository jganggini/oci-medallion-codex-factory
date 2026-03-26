# oci-medallion-qa

Usa esta skill para ejecutar QA de datos migrados en una arquitectura medallion sobre buckets y Autonomous Database, especialmente cuando el proyecto tenga contratos en `workspace/migration-input/<project_id>/quality/contracts/` y necesite evidencia por `run_id + slice_key`.

## Flujo

1. ubicar el contrato del dataset en `quality/contracts/`
2. perfilar el extracto o export Gold con `oci-data-quality-mcp --command profile-bucket-data`
3. ejecutar `oci-data-quality-mcp --command run-contract` con `workflow_id`, `run_id` y `slice_key` para validar bucket y artefactos ADB
4. antes del cutover, ejecutar el mismo contrato con `--runtime oci --oci-mode apply` para correr los SQL QA en Autonomous
5. cerrar con `oci-data-quality-mcp --command gate-migration`
6. revisar `workspace/oci-mirror/<env>/.../quality/` y `workspace/oci-mirror/<env>/.../autonomous_database/<adb>/control_plane/quality_results/`
