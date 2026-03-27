# OCI IAM Policy Baseline

## Objetivo

Definir una baseline IAM repetible para el factory medallion, separando:

- principals humanos que despliegan y operan
- runtime principals de los servicios OCI
- dynamic groups especificos para Autonomous Database y Data Catalog
- policies condicionadas para Data Integration y Data Flow

## Principios

1. crear o reutilizar primero el `compartment` compartido de la plataforma medallion por ambiente
2. crear `groups`, `dynamic groups` y `policies` antes de publicar buckets, ADB, red o jobs
3. usar alcance por `compartment` siempre que sea posible
4. reservar `tenancy` para `inspect compartments` y casos donde Oracle lo exige
5. separar las policies de operadores humanos de las policies runtime de los servicios
6. evitar matching rules amplias; preferir `resource.id = ...` o `resource.compartment.id = ...`

## Principals base

- operators group: `grp-medallion-operators-<env>`
- data flow admins group: `grp-dataflow-admin-<env>`
- ADB resource principal dynamic group: `dg-adb-resource-principal-<env>`
- Data Catalog dynamic group: `dg-data-catalog-harvest-<env>`
- Data Integration workspace principal: `ocid1.disworkspace...`

## Bundles recomendados

### Operators

```text
Allow group <operators_group> to inspect compartments in tenancy
Allow group <operators_group> to manage buckets in compartment <compartment_name>
Allow group <operators_group> to manage objects in compartment <compartment_name>
Allow group <operators_group> to manage autonomous-database-family in compartment <compartment_name>
Allow group <operators_group> to use virtual-network-family in compartment <compartment_name>
Allow group <operators_group> to manage dataflow-family in compartment <compartment_name>
Allow group <operators_group> to manage dis-family in compartment <compartment_name>
Allow group <operators_group> to manage data-catalog-family in compartment <compartment_name>
Allow group <operators_group> to manage data-catalog-private-endpoints in compartment <compartment_name>
Allow group <operators_group> to manage dataflow-private-endpoint in tenancy
Allow group <operators_group> to manage vaults in compartment <compartment_name>
Allow group <operators_group> to manage secret-family in compartment <compartment_name>
Allow group <operators_group> to read log-groups in compartment <compartment_name>
Allow group <operators_group> to read log-content in compartment <compartment_name>
Allow group <operators_group> to manage work-requests in compartment <compartment_name>
```

### Data Flow Private Access

```text
Allow group <dataflow_admin_group> to inspect compartments in tenancy
Allow group <dataflow_admin_group> to manage dataflow-family in compartment <compartment_name>
Allow group <dataflow_admin_group> to manage dataflow-private-endpoint in tenancy
Allow group <dataflow_admin_group> to use virtual-network-family in compartment <compartment_name>
Allow group <dataflow_admin_group> to read objectstorage-namespaces in tenancy
Allow group <dataflow_admin_group> to read buckets in compartment <compartment_name>
Allow group <dataflow_admin_group> to manage objects in compartment <compartment_name>
Allow group <dataflow_admin_group> to read log-groups in compartment <compartment_name>
Allow group <dataflow_admin_group> to use log-content in compartment <compartment_name>
```

### Autonomous Resource Principal Opcional

```text
Allow dynamic-group <adb_dynamic_group> to read objectstorage-namespaces in tenancy
Allow dynamic-group <adb_dynamic_group> to manage buckets in compartment <compartment_name>
Allow dynamic-group <adb_dynamic_group> to manage objects in compartment <compartment_name>
```

Matching rule recomendado:

```text
ALL {resource.type = 'autonomousdatabase', resource.compartment.id = '<project_compartment_ocid>'}
```

Nota:

- usa este bundle solo si ADB leerá Object Storage con resource principal; para cargas inmediatas del factory en el mismo rollout conviene usar pre-authenticated requests sobre los objetos gold

### Data Integration Workspace Runtime

```text
Allow any-user to use virtual-network-family in compartment <compartment_name> where all {request.principal.type='disworkspace', request.principal.id='<workspace_ocid>'}
Allow any-user to use secret-family in compartment <compartment_name> where all {request.principal.type='disworkspace', request.principal.id='<workspace_ocid>'}
Allow any-user to read secret-bundles in compartment <compartment_name> where all {request.principal.type='disworkspace', request.principal.id='<workspace_ocid>'}
Allow any-user to read objectstorage-namespaces in tenancy where all {request.principal.type='disworkspace', request.principal.id='<workspace_ocid>'}
Allow any-user to manage buckets in compartment <compartment_name> where all {request.principal.type='disworkspace', request.principal.id='<workspace_ocid>'}
Allow any-user to manage objects in compartment <compartment_name> where all {request.principal.type='disworkspace', request.principal.id='<workspace_ocid>'}
```

### Data Integration Private Workspace Bootstrap

```text
Allow service dataintegration to use virtual-network-family in compartment <compartment_name>
```

Nota:

- esta policy debe existir antes de crear un workspace DI privado
- sin ella, Oracle puede devolver `NotAuthorizedOrNotFound` sobre la VCN o subnet aunque el operador humano si tenga permisos
- deja la policy runtime condicionada por `disworkspace` para despues de que el workspace ya exista
- si el workspace DI necesita crear o borrar buckets, `manage objects` no es suficiente; agrega tambien `manage buckets`

### Data Catalog Harvest and Lineage

```text
Allow dynamic-group <data_catalog_dynamic_group> to read object-family in compartment <compartment_name>
Allow dynamic-group <data_catalog_dynamic_group> to read dis-workspaces-lineage in compartment <compartment_name>
Allow any-user to manage data-catalog-data-assets in compartment <compartment_name> where all {request.principal.type='dataflowrun', target.catalog.id='<catalog_ocid>', target.resource.kind='dataFlow'}
```

Matching rule recomendado:

```text
Any {resource.id = '<catalog_ocid>'}
```

## Notas del factory

- `iam_baseline` vive dentro de `project.medallion.yaml` para que intake, bootstrap y publish lo traten como parte del contrato del proyecto.
- `oci-iam-mcp` debe crear `groups`, `dynamic groups` y `policies` antes del resto de servicios.
- Si un ambiente comparte red, vault o logging fuera del compartment medallion principal, duplica las policies sobre el compartment compartido correspondiente en vez de ampliar permisos en tenancy.
