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
4. Coloca insumos en `workspace/migration-input/<project_id>/`, incluyendo `sql/`, `scripts/`, `data/`, `docs/`, `references/`, `samples/` y `exports/` segun aplique.
5. Si aun no copiaras algun insumo, registra la ruta fuente exacta y la ruta destino planeada dentro de `workspace/migration-input/<project_id>/`.
6. Completa `.local/oci/config` y `.local/oci/key.pem`.
7. Coloca wallets en `.local/autonomous/wallets/<env>/<adb_name>/`.
8. Levanta Docker con `docker compose up -d`.
9. Si quieres que Codex te guie paso a paso, abre `docs/codex-advisor.md` y empieza con `oci-medallion-advisor`.
10. Dentro del flujo guiado, Codex debe volver a ubicar este paso inmediatamente despues de discovery y del plan inicial si detecta que Docker aun no esta arriba.
11. Ejecuta `python scripts/migration_intake.py --repo-root . --project-id <project_id>`.
12. Revisa `workspace/migration-input/<project_id>/_inventory/inventory.md`.
13. Ajusta `project.medallion.yaml`, especialmente `deployment_scope`, `delivery_target`, `script_sources`, `data_sources`, `reference_doc_sources`, `pending_input_deliveries`, `existing_buckets`, `source_assets`, `control_plane`, `lineage` y `reprocess`.
14. Ejecuta las skills en este orden:
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
- rutas declaradas para SQL, scripts, data y documentacion de referencia
- inventario de insumos generado
- `project.medallion.yaml` con alcance por defecto hasta `gold_adb`, capas, buckets y assets correctamente descritos
- runtime Docker levantado antes de intake, bootstrap y publish
- bootstrap del control plane listo para ADB
- base lista para crear foundation OCI, scaffold del proyecto y QA por slice
