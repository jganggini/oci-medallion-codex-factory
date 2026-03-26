# oci-medallion-migration-intake

Usa esta skill para revisar `workspace/migration-input/<project_id>/`.

## Responsabilidades

- ejecutar `scripts/stage_local_assets.py` si los insumos aun estan fuera del repo
- ejecutar el inventory de insumos
- detectar faltantes de SQL, scripts, data, docs, referencias y muestras
- generar resumen de readiness
- bloquear scaffold si faltan insumos minimos

## Comando base

`python scripts/migration_intake.py --repo-root . --project-id <project_id>`

Si el usuario aun no copio los archivos al repo o faltan `config`, `.pem` y wallet en `.local/`, primero ejecuta `py -3 scripts/stage_local_assets.py --repo-root . --project-id <project_id> ...`.

## Salidas esperadas

- `workspace/migration-input/<project_id>/_inventory/inventory.json`
- `workspace/migration-input/<project_id>/_inventory/inventory.md`
- `workspace/migration-input/<project_id>/_inventory/context.json`
