---
name: oci-terraform-fallback
description: Consulta el provider oficial OCI de Terraform, ejemplos y Resource Discovery cuando un despliegue del factory no este claro, falle o requiera validar argumentos, dependencias y patrones oficiales antes de modificar Terraform o MCPs.
metadata:
  short-description: Fallback oficial para OCI Terraform
---

# OCI Terraform Fallback

Usa esta skill como plan B oficial para despliegues OCI con Terraform cuando el factory necesite confirmar como declarar un recurso, que dependencias exige, o como recuperar una configuracion existente sin inventar HCL.

## Cuando usarla

1. un `terraform plan` o `apply` falla por argumentos, dependencias o bloques `lifecycle`
2. no esta claro cual es el recurso OCI correcto o que atributos requiere
3. existe drift entre OCI y `infra/` y conviene reconstruir una base con Resource Discovery
4. un MCP genera un plan local pero necesitamos contrastarlo con la implementacion oficial del provider
5. queremos validar que la duda es del repo y no del servicio OCI o del recurso Terraform

## Flujo

1. identificar servicio y recurso probable
2. cargar `references/official-oci-terraform.md`
3. consultar en este orden:
   - provider y autenticacion
   - recurso exacto en Terraform Registry
   - ejemplo minimo o tutorial oficial
   - Resource Discovery si el recurso ya existe o hay drift
4. aplicar el hallazgo en `infra/modules/`, `infra/stacks/<env>/` o en el MCP que emite el plan
5. registrar el ajuste en manifests, reportes o contratos del proyecto cuando corresponda

## Atajos por servicio del factory

- network foundation: buscar recursos `vcn`, `subnet`, `network security group`, `nat gateway`, `service gateway` y tablas de ruteo
- object storage: buscar `bucket` y `object`
- data flow: buscar `application` y `run`
- data integration: buscar `workspace`, `project`, `folder`, `task` y `pipeline`
- autonomous database: buscar `autonomous database` y dependencias de red, secretos o wallet si el caso aplica

## Guardrails

- priorizar documentacion oficial Oracle y Terraform Registry antes que blogs o snippets
- no copiar ejemplos completos al repo sin antes reducirlos al caso del factory
- tratar Resource Discovery como punto de partida; revisar placeholders, `ignore_changes` y atributos sensibles antes de aplicar
- mantener separada la capa SQL: este skill ayuda con infraestructura y recursos OCI; usuarios, grants, tablas y DDL finales siguen en `oci-autonomous-database-mcp` y en los scripts SQL del proyecto
