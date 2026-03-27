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
- ejecutarse desde los wrappers Docker del repo cuando se corran localmente
- usar OCI CLI por Docker cuando entren en `runtime oci`

## Integracion con Codex

- El repo usa un `.codex/config.toml` opcional y local al proyecto, con nombres de servidor propios bajo el prefijo `oci_medallion_*`.
- Esos entries no reemplazan la configuracion global del usuario; solo agregan MCPs de este factory a nivel proyecto.
- `setup-dev.ps1` y `setup-dev.sh` sincronizan automaticamente `.codex/config.template.toml` hacia `.codex/config.toml`.
- Codex no consume directamente `mcp/servers/*/server.py` porque esos archivos son runtimes CLI del factory. El puente `.codex/factory_mcp_bridge.py` los expone por `stdio` como herramientas MCP validas.
- El bridge ejecuta los runtimes reales mediante `scripts/docker_repo_python.ps1` o `scripts/docker_repo_python.sh`, para mantener el flujo Docker-first del proyecto.

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
2. `docker compose up -d dev-base oci-runner dataflow-local` despues de discovery, del plan inicial y del staging
3. `oci-medallion-migration-intake`
4. `oci-medallion-bootstrap`
5. `oci-medallion-network-foundation`
6. `oci-medallion-scaffold`
7. `oci-medallion-publish`
8. `oci-medallion-qa`
9. `oci-terraform-fallback` cuando el provider OCI, el recurso Terraform o el drift no esten claros
10. `oci-medallion-validate`
11. `oci-medallion-incident` solo cuando exista un fallo operativo o de despliegue

## Orden de provisioning de recursos

Cuando el proyecto entra en despliegue real, la secuencia base debe ser:

1. compartment del proyecto
2. baseline IAM del proyecto: groups, dynamic groups y policies por servicio
3. buckets `landing_external`, `bronze_raw`, `silver_trusted` y `gold_refined`
4. Autonomous Database y control plane
5. red del proyecto
6. carga de archivos a landing cuando aplique
7. Data Flow
8. Data Integration
9. QA y validacion general
