# Project Contract

## Fuente de verdad

Cada proyecto medallion debe definir un `project.medallion.yaml`.

## Campos minimos

- `project_id`
- `domain`
- `environment`
- `deployment_scope`
- `delivery_target`
- `migration_input_root`
- `sql_sources`
- `script_sources`
- `data_sources`
- `doc_sources`
- `sample_sources`
- `reference_doc_sources`
- `pending_input_deliveries`
- `target_layers`
- `existing_buckets`
- `source_assets`
- `iam_baseline`
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

- `deployment_scope` debe ser `end_to_end_gold` por defecto. Solo debe declararse un alcance parcial cuando el usuario o el proyecto lo pidan explicitamente.
- `delivery_target` debe ser `gold_adb` por defecto y representar la entrega final del proyecto en Autonomous Database.
- `migration_input_root` debe apuntar a `workspace/migration-input/<project_id>/`.
- `sql_sources`, `script_sources`, `data_sources`, `doc_sources`, `sample_sources` y `reference_doc_sources` deben ser relativos a `migration_input_root`.
- `pending_input_deliveries` debe registrar cualquier archivo prometido durante la entrevista que aun no fue copiado al repo, con `kind`, `source_path`, `target_path` y `status`.
- `pending_input_deliveries.kind` debe usar valores como `sql`, `scripts`, `data`, `references`, `samples` o `exports`.
- `pending_input_deliveries.target_path` debe apuntar solo a `workspace/migration-input/<project_id>/...`.
- `provisioning_order` debe respetar por defecto esta secuencia: `compartment -> iam_policies -> storage_layers -> autonomous -> network -> landing_ingestion -> data_flow -> data_integration -> validation`.
- `target_layers` debe diferenciar `landing_external`, `bronze_raw`, `silver_trusted`, `gold_refined` y `gold_adb`.
- `target_layers.gold_adb` debe quedar en `true` cuando `deployment_scope` sea `end_to_end_gold`.
- `existing_buckets` no puede usarse para inferir que todas las capas ya existen.
- Cada bucket o asset existente debe indicar `layer`, `managed_by_factory` e `ingestion_outside_flow`.
- `network_profile.compartment_name` debe representar el compartment compartido de la plataforma medallion por ambiente, normalmente `data-medallion-<env>`, no un compartment por proyecto.
- Los buckets administrados por el factory deben reutilizar nombres fijos por capa: `bucket-landing-external`, `bucket-bronze-raw`, `bucket-silver-trusted` y `bucket-gold-refined`.
- El aislamiento por proyecto dentro de capas compartidas debe hacerse por prefijos u objetos particionados dentro del bucket, por ejemplo `projects/<project_id>/...` o `source_system=.../entity=.../business_date=.../batch_id=...`.
- `iam_baseline` debe declarar al menos un grupo operador, los `dynamic groups` requeridos por ADB y Data Catalog cuando esos servicios esten habilitados, y bundles de policies por servicio.
- `iam_baseline` debe usar alcance por compartment salvo que Oracle requiera tenancy, como en `inspect compartments`.
- Si `di_pipeline.enabled` es `true`, el manifiesto debe registrar el `workspace_ocid` o su placeholder y las policies condicionadas por `request.principal.type='disworkspace'`.
- Si `autonomous_profile.enabled` es `true`, el manifiesto debe declarar un `dynamic group` para el resource principal de ADB.
- `control_plane.database_name` debe apuntar al ADB que centraliza `workflow_id`, `run_id`, `slice_key`, checkpoints, QA y lineage outbox.
- `control_plane.partition_pattern` debe soportar replay por `entity + business_date + batch_id`.
- `lineage.strategy` debe declarar si el proyecto usa lineage nativo, custom o hibrido.
- Si `data_catalog.enabled` es `true`, el manifiesto debe aclarar si se cosechara Object Storage, Autonomous y DI.
- `reprocess.default_scope` debe ser `run+slice` salvo que el proyecto justifique otra estrategia.
- `quality_profile` debe apuntar a contratos y SQL de QA por dataset cuando el proyecto requiera gate de migracion.
- `config`, `.pem` y wallets siguen fuera del manifiesto, pero deben stagearse a `.local/` antes de bootstrap o publish.
- Los jobs o loaders que publiquen en `gold_adb` deben declarar si cargan desde bucket usando `DBMS_CLOUD.COPY_DATA` o mediante un procedimiento ADB invocado sobre `file_uri_list`.

## Flujo

1. Intake valida los insumos canonicos.
2. El manifiesto del proyecto fija buckets existentes, assets fuente, capas objetivo, insumos de `sql/scripts/data/references` y estrategia de migracion con ruta normal `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb`.
3. Bootstrap asegura primero el compartment compartido de la plataforma medallion por ambiente y luego el baseline IAM: groups, dynamic groups y policies por servicio.
4. Bootstrap asegura los buckets base por capa, despues ADB y deja lista la secuencia para red y publicacion.
5. Scaffold prepara Data Flow, Data Integration, SQL y contratos de calidad respetando la ruta `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb`.
6. Publish ejecuta la red requerida, sube archivos y artefactos bajo prefijos del proyecto dentro de las capas compartidas, registra checkpoints, lineage y sincroniza el espejo OCI.
7. QA valida por `run_id + slice_key` antes del cutover o del reproceso parcial.
