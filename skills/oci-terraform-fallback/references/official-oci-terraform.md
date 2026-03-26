# Official OCI Terraform Reference Pack

Usa esta referencia cuando una definicion Terraform del factory no sea suficiente o falle y necesitemos contrastarla con material oficial.

## Orden recomendado de consulta

1. `Set Up OCI Terraform`
   - base para provider, autenticacion y versionado compatible
   - https://docs.oracle.com/en-us/iaas/Content/dev/terraform/tutorials/tf-provider.htm
2. `Terraform Registry - OCI provider`
   - indice oficial para recursos y data sources del provider
   - https://registry.terraform.io/providers/oracle/oci/latest/docs
3. `Examples, Templates, and Solutions`
   - ejemplos minimos por servicio y plantillas de Resource Manager
   - https://docs.oracle.com/iaas/Content/dev/terraform/examples-templates.htm
4. `Applying Configurations`
   - criterios de `terraform init`, `plan` y `apply`
   - https://docs.oracle.com/en-us/iaas/Content/terraform/applying.htm
5. `Resource Discovery`
   - exportar configuracion y estado Terraform desde recursos OCI existentes
   - https://docs.oracle.com/iaas/Content/terraform/resource-discovery.htm
6. `Using Resource Discovery`
   - comandos, prerequisitos y export por compartment o servicio
   - https://docs.oracle.com/en-us/iaas/Content/dev/terraform/resource-discovery-using.htm
7. `Services Reference`
   - verificar si el servicio y Resource Discovery estan soportados
   - https://docs.oracle.com/iaas/Content/dev/terraform/supported-services.htm
8. `Terraform Provider Tutorials`
   - tutoriales paso a paso para escenarios simples
   - https://docs.oracle.com/en-us/iaas/Content/dev/terraform/tutorials.htm

## Busquedas sugeridas por servicio

- network foundation:
  - `site:registry.terraform.io oracle oci vcn subnet network security group nat gateway service gateway route table`
- object storage:
  - `site:registry.terraform.io oracle oci objectstorage bucket object`
- data flow:
  - `site:registry.terraform.io oracle oci dataflow application run`
- data integration:
  - `site:registry.terraform.io oracle oci dataintegration workspace project folder task pipeline`
- autonomous database:
  - `site:registry.terraform.io oracle oci autonomous database`

## Como aterrizar el hallazgo en este repo

1. si el cambio es estructural, ajustar `infra/modules/` o `infra/stacks/<env>/`
2. si el cambio es operativo, reflejarlo en el MCP que genera el plan OCI local
3. si el recurso ya existe y el repo no lo modela bien, ejecutar primero un analisis de Resource Discovery y luego depurar el HCL
4. si el problema es de datos o SQL y no de infraestructura, volver a `oci-autonomous-database-mcp` o `oci-medallion-qa`

## Criterios para elegir la fuente correcta

- usa Terraform Registry cuando ya conoces el recurso exacto y necesitas argumentos o atributos
- usa `Examples, Templates, and Solutions` cuando necesitas una muestra minima y oficial
- usa `Resource Discovery` cuando el recurso ya existe, hay drift o se requiere reconstruir una base Terraform
- usa tutoriales cuando el equipo necesita una secuencia guiada para validar el flujo end to end
