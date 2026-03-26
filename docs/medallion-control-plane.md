# Medallion Control Plane

## Objetivo

Definir un modelo operacional comun para Data Flow, Data Integration, Autonomous Database, Object Storage, QA y Data Catalog.

## Claves estandar

- `workflow_id`
- `run_id`
- `parent_run_id`
- `entity_name`
- `layer`
- `slice_key`
- `business_date`
- `batch_id`
- `watermark_low`
- `watermark_high`
- `reprocess_request_id`
- `source_asset_ref`
- `target_asset_ref`
- `service_run_ref`

## Tablas de control recomendadas

- `ctl_workflow`
- `ctl_entity`
- `ctl_run`
- `ctl_run_step`
- `ctl_run_slice`
- `ctl_checkpoint`
- `ctl_reprocess_request`
- `ctl_quality_result`
- `ctl_lineage_outbox`

## Estrategia de reproceso

- granularidad por defecto: `run+slice`
- slice recomendado: `entity={entity}/business_date={business_date}/batch_id={batch_id}`
- no asumir reproceso fila a fila como flujo normal
- reutilizar checkpoints sanos antes de relanzar una particion

## Lineage

- nativo: Data Flow y Data Integration cuando OCI lo entregue
- custom: SQL en ADB, cargas `DBMS_CLOUD`, transformaciones propias y eventos de reproceso
- publicacion: outbox en el control plane y luego `oci-data-catalog-mcp --command import-openlineage`

## QA

Ejecutar contratos y gates por `run_id + slice_key` para poder:

- bloquear promociones
- comparar reprocesos parciales
- dejar evidencia reutilizable en el control plane
