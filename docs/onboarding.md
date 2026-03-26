# Onboarding

## Requisitos

- Codex App ya instalado y autenticado fuera del repo
- Cursor o VS Code
- Docker Desktop
- Python 3.11 o superior
- Git
- Acceso OCI y credenciales locales

## Flujo recomendado

1. Clona el repo.
2. Ejecuta `setup-dev.ps1` o `setup-dev.sh`.
3. Crea o elige un `project_id`.
4. Coloca insumos en `workspace/migration-input/<project_id>/`.
5. Si tienes insumos sensibles, guardalos primero en `.local/migration-private/<project_id>/` y copia solo la version sanitizada al workspace.
6. Completa `.local/oci/config` y `.local/oci/key.pem`.
7. Coloca wallets en `.local/autonomous/wallets/<env>/<adb_name>/`.
8. Levanta Docker con `docker compose up -d`.
9. Si quieres que Codex te guie paso a paso, abre `docs/codex-advisor.md` y empieza con `oci-medallion-advisor`.
10. Ejecuta `python scripts/migration_intake.py --repo-root . --project-id <project_id>`.
11. Revisa `workspace/migration-input/<project_id>/_inventory/inventory.md`.
12. Ajusta `project.medallion.yaml`, especialmente `existing_buckets`, `source_assets`, `control_plane`, `lineage` y `reprocess`.
13. Ejecuta las skills en este orden:
    - `oci-medallion-advisor`
    - `oci-medallion-migration-intake`
    - `oci-medallion-bootstrap`
    - `oci-medallion-network-foundation`
    - `oci-medallion-scaffold`
    - `oci-medallion-publish`
    - `oci-medallion-qa`
    - `oci-terraform-fallback` si Terraform o un recurso OCI no estan claros o hay drift
    - `oci-medallion-validate`

## Resultado esperado

Al terminar el onboarding, el repo debe tener:

- `.local/` inicializado sin secretos reales versionados
- `workspace/oci-mirror/` inicializado por ambiente
- `workspace/migration-input/<project_id>/` listo para intake
- inventario de insumos generado
- `project.medallion.yaml` con capas, buckets y assets correctamente descritos
- bootstrap del control plane listo para ADB
- base lista para crear foundation OCI, scaffold del proyecto y QA por slice
