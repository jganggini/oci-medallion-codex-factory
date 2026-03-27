# OCI Mirror Contract

## Objetivo

`workspace/oci-mirror/` permite compartir con clientes y equipos una vista local del proyecto alineada con la organizacion real en OCI, sin exponer secretos.

## Layout

`workspace/oci-mirror/<env>/compartment-data-medallion-<env>/`

Ese layout representa el compartment compartido de la plataforma medallion por ambiente. Los proyectos no deben crear un compartment nuevo por cada despliegue; deben convivir dentro de esta estructura y diferenciarse por prefijos u objetos dentro de cada capa.

Servicios esperados:

- `iam/`
- `network/`
- `buckets/`
- `data_flow/`
- `data_integration/`
- `autonomous_database/`
- `quality/`
- `vault/`
- `reports/`

## Contenido esperado

- JSON de metadatos
- manifests efectivos
- artefactos publicables
- referencias a configuracion local
- reportes redacted
- evidencias de despliegue
- contratos QA, perfiles de bucket, resultados de checks y gates de migracion

## Contenido prohibido

- llaves privadas
- wallets reales
- passwords
- secretos
- `.env` reales

## Convencion de buckets

Dentro de `buckets/` deben existir como minimo:

- `bucket-landing-external/`
- `bucket-bronze-raw/`
- `bucket-silver-trusted/`
- `bucket-gold-refined/`

Dentro de cada bucket, los artefactos del proyecto deben quedar aislados por prefijos, por ejemplo `objects/projects/<project_id>/...`.
