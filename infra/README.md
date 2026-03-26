# Infra

Infraestructura base Terraform compatible con OCI Resource Manager.

## Estructura

- `modules/`
  Modulos reutilizables por servicio.
- `stacks/dev/`
- `stacks/qa/`
- `stacks/prod/`

## Modulos previstos

- `compartment-foundation`
- `network-foundation`
- `object-storage-foundation`
- `data-flow-foundation`
- `data-integration-foundation`
- `autonomous-database-foundation`
- `vault-foundation`

## Enfoque

Los modulos incluidos en este scaffold definen contratos, variables y outputs base para que el equipo pueda completar la logica real de OCI sin perder consistencia entre ambientes.
