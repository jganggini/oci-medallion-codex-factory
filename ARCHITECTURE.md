# Architecture

## Objetivo

El factory separa claramente insumos, configuracion sensible, infraestructura OCI y automatizaciones para que los proyectos medallion sean repetibles y auditables.

## Capas del repositorio

### Foundation

- `infra/`
- `templates/project.medallion.yaml`

Define compartments por ambiente, buckets por capa, red base, Data Flow, Data Integration, Autonomous Database, Vault y outputs reutilizables para Resource Manager.

### Automation

- `mcp/`
- `skills/`
- `scripts/`

Convierte la base declarativa en operaciones repetibles para bootstrap, intake, scaffold, publicacion, validacion y diagnostico.

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

- `raw`
- `trusted`
- `refined`

### Servicios por ambiente

- IAM y policies del ambiente
- VCN, subredes privadas, NSGs, gateways y private endpoints
- Data Flow
- Data Integration
- Autonomous Database
- Vault

## Contratos operativos

### Proyecto

`project.medallion.yaml` define:

- identidad del proyecto
- dominio
- ambiente
- ubicacion de los insumos de migracion
- perfiles de red
- perfiles de Autonomous
- jobs Data Flow
- pipeline de Data Integration
- reglas de validacion
- aprobaciones requeridas

### Espejo OCI

`workspace/oci-mirror/<env>/compartment-data-medallion-<env>/` contiene:

- `iam/`
- `network/`
- `buckets/`
- `data_flow/`
- `data_integration/`
- `autonomous_database/`
- `vault/`
- `reports/`

Cada carpeta debe almacenar manifests efectivos, JSON redacted, artefactos publicables y referencias tecnicas, nunca secretos reales.

## Flujo esperado

1. El usuario coloca SQL, docs y muestras en `workspace/migration-input/<project_id>/`.
2. `migration-intake` valida el contrato y genera inventario estructurado.
3. El manifiesto del proyecto fija el contrato del caso de uso.
4. La foundation OCI se provisiona con Terraform compatible con Resource Manager.
5. Los MCPs crean o actualizan recursos y sincronizan el espejo local de OCI.
6. Las skills de Codex orquestan bootstrap, scaffold, publicacion y validacion.
7. Los equipos revisan el espejo local y los artefactos antes de promover cambios.
