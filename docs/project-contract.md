# Project Contract

## Fuente de verdad

Cada proyecto medallion debe definir un `project.medallion.yaml`.

## Campos minimos

- `project_id`
- `domain`
- `environment`
- `migration_input_root`
- `sql_sources`
- `doc_sources`
- `sample_sources`
- `target_layers`
- `network_profile`
- `autonomous_profile`
- `dataflow_jobs`
- `di_pipeline`
- `validation_rules`
- `approvals`

## Reglas

- `migration_input_root` debe apuntar a `workspace/migration-input/<project_id>/`.
- Los `sql_sources`, `doc_sources` y `sample_sources` deben ser relativos a `migration_input_root`.
- Las reglas de validacion deben reflejar los minimos requeridos por el intake.
- Si el proyecto usa Autonomous, el perfil debe indicar wallet, estrategia de carga y objetos principales.

## Flujo

1. Intake valida los insumos.
2. El manifiesto se completa o ajusta.
3. Bootstrap crea foundation OCI.
4. Scaffold genera el proyecto.
5. Publish publica artefactos y actualiza el espejo OCI.
