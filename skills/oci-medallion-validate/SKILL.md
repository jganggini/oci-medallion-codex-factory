# oci-medallion-validate

Usa esta skill para validar contratos, artefactos, data quality y espejo OCI.

## Flujo

1. ejecutar `scripts/docker_repo_python.ps1 scripts/validate_factory.py --repo-root .` o `scripts/docker_repo_python.sh scripts/validate_factory.py --repo-root .`
2. revisar el intake y el contexto generado
3. verificar que existan manifests en buckets, Data Flow, DI, ADB y Data Catalog
4. validar que el control plane tenga `runs`, `steps`, `slices`, `checkpoints` y `lineage_outbox` cuando el proyecto los requiera
5. ejecutar contratos de QA con `oci-data-quality-mcp` cuando el proyecto tenga `quality/contracts/`
6. confirmar que no existan secretos versionados
7. si aparece drift o una configuracion OCI no cierra con Terraform, consultar `oci-terraform-fallback` y decidir si conviene ejemplo oficial o Resource Discovery
