# MCPs and Skills

## MCPs por servicio

El factory define estos MCPs:

- `oci-iam-mcp`
- `oci-network-mcp`
- `oci-object-storage-mcp`
- `oci-resource-manager-mcp`
- `oci-data-flow-mcp`
- `oci-data-integration-mcp`
- `oci-autonomous-database-mcp`
- `oci-data-catalog-mcp`
- `oci-data-quality-mcp`
- `oci-vault-mcp`
- `migration-intake-mcp`

Cada MCP debe:

- operar sobre OCI o sobre el espejo local
- producir manifests o reportes redacted
- actualizar `workspace/oci-mirror/`
- aceptar `workflow_id`, `run_id`, `slice_key` y metadatos operacionales cuando aplique

## Skills

- `oci-medallion-advisor`
- `oci-medallion-bootstrap`
- `oci-medallion-migration-intake`
- `oci-medallion-network-foundation`
- `oci-medallion-scaffold`
- `oci-medallion-publish`
- `oci-medallion-qa`
- `oci-terraform-fallback`
- `oci-medallion-validate`
- `oci-medallion-incident`

## Orden recomendado

1. `oci-medallion-advisor`
2. `docker compose up -d` despues de discovery y del plan inicial
3. `oci-medallion-migration-intake`
4. `oci-medallion-bootstrap`
5. `oci-medallion-network-foundation`
6. `oci-medallion-scaffold`
7. `oci-medallion-publish`
8. `oci-medallion-qa`
9. `oci-terraform-fallback` cuando el provider OCI, el recurso Terraform o el drift no esten claros
10. `oci-medallion-validate`
11. `oci-medallion-incident` solo cuando exista un fallo operativo o de despliegue
