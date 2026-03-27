# Onboarding

## Requisitos

- Codex App ya instalado y autenticado fuera del repo
- Cursor o VS Code
- Docker Desktop
- Git
- Acceso OCI y credenciales locales
- No se requiere Python ni OCI CLI instalados en host para el flujo recomendado

## Flujo recomendado

1. Clona el repo y abre esta carpeta local como workspace de trabajo.
2. Ejecuta `setup-dev.ps1 -ProjectId <project_id>` o `./setup-dev.sh <project_id>`.
3. Crea o elige un `project_id`.
4. Reune las rutas origen de SQL, scripts, data, docs, referencias, `config`, `.pem` y wallet si aplica.
5. Ejecuta `powershell -ExecutionPolicy Bypass -File .\scripts\docker_stage_assets.ps1 --project-id <project_id> ...` o `./scripts/docker_stage_assets.sh --project-id <project_id> ...` para copiarlos automaticamente a la ruta correcta del proyecto.
6. Si el runtime base aun no esta arriba, levanta Docker con `docker compose up -d dev-base oci-runner dataflow-local`.
7. Si quieres que Codex te guie paso a paso, abre `docs/codex-advisor.md` y empieza con `oci-medallion-advisor`.
8. Dentro del flujo guiado, Codex debe volver a ubicar este paso inmediatamente despues de discovery, del plan inicial y del staging si detecta que Docker aun no esta arriba.
9. Ejecuta `powershell -ExecutionPolicy Bypass -File .\scripts\docker_repo_python.ps1 scripts/migration_intake.py --repo-root . --project-id <project_id>` o `./scripts/docker_repo_python.sh scripts/migration_intake.py --repo-root . --project-id <project_id>`.
10. Revisa `workspace/migration-input/<project_id>/_inventory/inventory.md`.
11. Ajusta `project.medallion.yaml`, especialmente `deployment_scope`, `delivery_target`, `script_sources`, `data_sources`, `reference_doc_sources`, `pending_input_deliveries`, `existing_buckets`, `source_assets`, `control_plane`, `lineage` y `reprocess`.
12. Antes de bootstrap, asegurate de que `config`, `.pem` y wallets requeridos ya fueron stageados a `.local/`.
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
- rutas confirmadas para `config`, `.pem` y wallets requeridos
- `workspace/oci-mirror/` inicializado por ambiente
- `workspace/migration-input/<project_id>/` listo para intake
- rutas declaradas para SQL, scripts, data y documentacion de referencia
- stage automatico ejecutado para mover insumos y credenciales locales a su ruta correcta
- inventario de insumos generado
- `project.medallion.yaml` con alcance por defecto hasta `gold_adb`, capas, buckets y assets correctamente descritos
- runtime Docker levantado antes de intake, bootstrap y publish
- OCI CLI ejecutandose siempre por Docker
- bootstrap del control plane listo para ADB
- base lista para crear foundation OCI, scaffold del proyecto y QA por slice
