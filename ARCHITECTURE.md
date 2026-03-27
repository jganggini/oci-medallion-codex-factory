# Architecture

## Objetivo

El factory separa claramente insumos, configuracion sensible, infraestructura OCI y automatizaciones para que los proyectos medallion sean repetibles, auditables y reprocesables por slice.

## Capas del repositorio

### Foundation

- `infra/`
- `templates/project.medallion.yaml`
- `templates/autonomous/control_plane_bootstrap.sql`

Define compartments por ambiente, buckets por capa, red base, Data Flow, Data Integration, Autonomous Database, Data Catalog, Vault y outputs reutilizables para Resource Manager.

### Automation

- `mcp/`
- `skills/`
- `scripts/`

Convierte la base declarativa en operaciones repetibles para bootstrap, scaffold, publicacion, lineage, QA, reprocesos y diagnostico.

### Workspace local

- `workspace/migration-input/`
- `workspace/oci-mirror/`
- `.local/`

Separa insumos, espejo operativo y material sensible. `workspace/migration-input/` es la fuente canonica para Codex y las skills. `.local/migration-private/` sirve como landing zone privada opcional.

## Organizacion OCI recomendada

### Compartments por ambiente

- `data-medallion-dev`
- `data-medallion-qa`
- `data-medallion-prod`

### Buckets por capa

- `landing_external`
- `bronze_raw`
- `silver_trusted`
- `gold_refined`

### Servicios por ambiente

- IAM y policies del ambiente
- VCN, subredes privadas, NSGs, gateways y private endpoints
- Object Storage
- Data Flow
- Data Integration
- Autonomous Database
- Data Catalog
- Vault

## Control plane operacional

Autonomous Database actua como control plane transversal y debe concentrar:

- `workflow_id`, `run_id`, `parent_run_id`
- `slice_key`, `business_date`, `batch_id`
- `watermark_low`, `watermark_high`
- checkpoints reutilizables por entidad y capa
- solicitudes de reproceso parcial
- resultados de QA por run y slice
- outbox de lineage para publicacion OpenLineage

El schema recomendado es `MDL_CTL` y el bootstrap vive en `templates/autonomous/control_plane_bootstrap.sql`.

## Contratos operativos

### Proyecto

`project.medallion.yaml` define:

- identidad del proyecto
- capas objetivo
- buckets existentes y si son externos al flujo
- source assets
- baseline IAM del proyecto
- perfiles de red
- control plane
- perfiles de Autonomous
- jobs Data Flow
- pipeline de Data Integration
- estrategia Data Catalog y lineage
- estrategia de reproceso
- reglas de validacion y QA

### Espejo OCI

`workspace/oci-mirror/<env>/compartment-data-medallion-<env>/` contiene:

- `iam/`
- `network/`
- `buckets/`
- `data_flow/`
- `data_integration/`
- `autonomous_database/`
- `data_catalog/`
- `quality/`
- `vault/`
- `reports/`

Cada carpeta debe almacenar manifests efectivos, JSON redacted, artefactos publicables y referencias tecnicas, nunca secretos reales.

## Flujo esperado

1. El usuario coloca SQL, docs y muestras en `workspace/migration-input/<project_id>/`.
2. `migration-intake` valida el contrato y genera inventario estructurado.
3. El manifiesto fija capas, buckets existentes, assets fuente y estrategia operacional.
4. La foundation OCI se provisiona con Terraform compatible con Resource Manager.
5. El baseline IAM crea groups, dynamic groups y policies antes de publicar el resto de servicios.
6. Los MCPs crean o actualizan recursos y sincronizan el espejo local de OCI.
7. El control plane registra runs, steps, slices, checkpoints, QA y lineage outbox.
8. Data Catalog cosecha metadata nativa y recibe lineage custom para SQL, cargas y reprocesos.
9. Los equipos revisan el espejo local y los artefactos antes de promover cambios.
