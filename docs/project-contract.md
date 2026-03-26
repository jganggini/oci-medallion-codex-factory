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
- `existing_buckets`
- `source_assets`
- `network_profile`
- `control_plane`
- `autonomous_profile`
- `dataflow_jobs`
- `di_pipeline`
- `data_catalog`
- `lineage`
- `reprocess`
- `quality_profile`
- `validation_rules`
- `approvals`

## Reglas

- `migration_input_root` debe apuntar a `workspace/migration-input/<project_id>/`.
- `sql_sources`, `doc_sources` y `sample_sources` deben ser relativos a `migration_input_root`.
- `target_layers` debe diferenciar `landing_external`, `bronze_raw`, `silver_trusted`, `gold_refined` y `gold_adb`.
- `existing_buckets` no puede usarse para inferir que todas las capas ya existen.
- Cada bucket o asset existente debe indicar `layer`, `managed_by_factory` e `ingestion_outside_flow`.
- `control_plane.database_name` debe apuntar al ADB que centraliza `workflow_id`, `run_id`, `slice_key`, checkpoints, QA y lineage outbox.
- `control_plane.partition_pattern` debe soportar replay por `entity + business_date + batch_id`.
- `lineage.strategy` debe declarar si el proyecto usa lineage nativo, custom o hibrido.
- Si `data_catalog.enabled` es `true`, el manifiesto debe aclarar si se cosechara Object Storage, Autonomous y DI.
- `reprocess.default_scope` debe ser `run+slice` salvo que el proyecto justifique otra estrategia.
- `quality_profile` debe apuntar a contratos y SQL de QA por dataset cuando el proyecto requiera gate de migracion.

## Flujo

1. Intake valida los insumos canonicos.
2. El manifiesto del proyecto fija buckets existentes, assets fuente, capas objetivo y estrategia de migracion.
3. Bootstrap crea foundation OCI y el control plane operacional en ADB.
4. Scaffold prepara Data Flow, Data Integration, SQL y contratos de calidad.
5. Publish registra artefactos, checkpoints, lineage y sincroniza el espejo OCI.
6. QA valida por `run_id + slice_key` antes del cutover o del reproceso parcial.
