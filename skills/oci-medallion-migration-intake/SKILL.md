# oci-medallion-migration-intake

Usa esta skill para revisar `workspace/migration-input/<project_id>/`.

## Responsabilidades

- ejecutar el inventory de insumos
- detectar faltantes de SQL, docs y muestras
- generar resumen de readiness
- bloquear scaffold si faltan insumos minimos

## Comando base

`python scripts/migration_intake.py --repo-root . --project-id <project_id>`

## Salidas esperadas

- `workspace/migration-input/<project_id>/_inventory/inventory.json`
- `workspace/migration-input/<project_id>/_inventory/inventory.md`
- `workspace/migration-input/<project_id>/_inventory/context.json`
