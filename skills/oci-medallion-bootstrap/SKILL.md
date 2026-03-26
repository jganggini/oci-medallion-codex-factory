# oci-medallion-bootstrap

Usa esta skill para preparar la base de un proyecto medallion nuevo.

## Flujo

1. validar el scaffold con `py -3 scripts/validate_factory.py --repo-root .`
2. inicializar el workspace con `setup-dev.ps1` o `setup-dev.sh`
3. confirmar `project.medallion.yaml`
4. revisar `workspace/oci-mirror/<env>/`
5. crear buckets base con el MCP de Object Storage
