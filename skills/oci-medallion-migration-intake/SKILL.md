# oci-medallion-migration-intake

Usa esta skill para revisar `workspace/migration-input/<project_id>/`.

## Responsabilidades

- ejecutar `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` si los insumos aun estan fuera del repo
- ejecutar el inventory de insumos
- detectar faltantes de SQL, scripts, data, docs, referencias y muestras
- generar resumen de readiness
- bloquear scaffold si faltan insumos minimos

## Comando base

`scripts/docker_repo_python.ps1 scripts/migration_intake.py --repo-root . --project-id <project_id>` o `scripts/docker_repo_python.sh scripts/migration_intake.py --repo-root . --project-id <project_id>`

Si el usuario aun no copio los archivos al repo o faltan `config`, `.pem` y wallet en `.local/`, primero ejecuta `scripts/docker_stage_assets.ps1 --project-id <project_id> ...` o `scripts/docker_stage_assets.sh --project-id <project_id> ...`.

## Salidas esperadas

- `workspace/migration-input/<project_id>/_inventory/inventory.json`
- `workspace/migration-input/<project_id>/_inventory/inventory.md`
- `workspace/migration-input/<project_id>/_inventory/context.json`
