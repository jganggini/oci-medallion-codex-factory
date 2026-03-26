# oci-medallion-bootstrap

Usa esta skill para preparar la base de un proyecto medallion nuevo.

## Flujo

1. validar el scaffold con `py -3 scripts/validate_factory.py --repo-root .`
2. inicializar el workspace con `setup-dev.ps1` o `setup-dev.sh`
3. confirmar `project.medallion.yaml`
4. revisar `workspace/oci-mirror/<env>/`
5. crear buckets base con el MCP de Object Storage
6. si el proyecto usa ADW, registrar `create-adb-definition` y preparar el bootstrap de usuario/tablas con `create-database-user`, `bootstrap-schema` y `apply-sql`
7. si Terraform o el recurso OCI no estan claros, consultar `oci-terraform-fallback` antes de cambiar `infra/` o un MCP
