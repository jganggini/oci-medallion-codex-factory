# oci-medallion-publish

Usa esta skill para publicar artefactos en el espejo OCI y dejar todo listo para una futura publicacion real.

Asume publicacion end-to-end hasta `gold_adb` por defecto. Solo recorta el alcance si el manifiesto o el usuario lo indican explicitamente.

## Flujo

1. confirmar que el compartment del proyecto ya existe o crearlo primero con `oci-iam-mcp`
2. crear el baseline IAM del proyecto antes de cualquier otro servicio: groups, dynamic groups y policies declaradas en `iam_baseline`
3. crear o sincronizar los buckets `landing_external`, `bronze_raw`, `silver_trusted` y `gold_refined` con `oci-object-storage-mcp`
4. registrar ADB, bootstrapear `MDL_CTL`, crear usuario, aplicar DDL y dejar lista la entrega final en `gold_adb` con `oci-autonomous-database-mcp`
5. crear la VCN, subredes, route tables y NSGs del proyecto con `oci-network-mcp`
6. subir los archivos fuente a `landing_external` cuando la carga ocurra dentro del factory; si llega por fuera, sincronizar el bucket o asset como existente
7. empaquetar y crear las aplicaciones Data Flow por salto de capa con `oci-data-flow-mcp`, incluyendo el movimiento final hacia `gold_refined` o el loader a `gold_adb` cuando corresponda
8. cuando `gold_refined` viva en Object Storage, preferir `load-gold-object` con `--source-uri/--file-uri-list` y un procedimiento ADB o `DBMS_CLOUD.COPY_DATA` antes que copiar archivos manualmente
9. crear workspace, proyectos, folders, tasks y pipeline de Data Integration con `oci-data-integration-mcp`
10. registrar Data Catalog, assets, harvests e importar lineage cuando aplique
11. ejecutar QA contractual y validaciones generales con `oci-data-quality-mcp`
12. verificar reportes en `workspace/oci-mirror/<env>/.../reports/`, `quality/`, `data_catalog/` y `autonomous_database/<adb>/control_plane/`
13. si algun recurso o plan Terraform no coincide con el servicio OCI, consultar `oci-terraform-fallback` antes de modificar el despliegue
