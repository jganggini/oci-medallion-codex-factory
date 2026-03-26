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
- `oci-vault-mcp`
- `migration-intake-mcp`

Cada MCP debe:

- operar sobre OCI
- producir manifests o reportes redacted
- actualizar `workspace/oci-mirror/`

## Skills

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

1. `oci-medallion-migration-intake`
2. `oci-medallion-bootstrap`
3. `oci-medallion-network-foundation`
4. `oci-medallion-scaffold`
5. `oci-medallion-publish`
6. `oci-medallion-qa`
7. `oci-terraform-fallback` cuando el provider OCI, el recurso Terraform o el drift no esten claros
8. `oci-medallion-validate`
9. `oci-medallion-incident` solo cuando exista un fallo operativo o de despliegue
