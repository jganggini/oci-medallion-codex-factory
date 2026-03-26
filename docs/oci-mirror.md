# OCI Mirror Contract

## Objetivo

`workspace/oci-mirror/` permite compartir con clientes y equipos una vista local del proyecto alineada con la organizacion real en OCI, sin exponer secretos.

## Layout

`workspace/oci-mirror/<env>/compartment-data-medallion-<env>/`

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

- `bucket-raw/`
- `bucket-trusted/`
- `bucket-refined/`
