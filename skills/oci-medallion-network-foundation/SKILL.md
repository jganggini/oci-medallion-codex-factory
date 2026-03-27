# oci-medallion-network-foundation

Usa esta skill para preparar la foundation de red del ambiente.

En la secuencia estandar del factory esta etapa ocurre despues de crear el compartment del proyecto, los buckets base y Autonomous Database.

## Flujo

1. revisar `infra/stacks/<env>/main.tf`
2. confirmar que la red se creara dentro del compartment del proyecto, nunca en tenancy root
3. confirmar VCN, subredes privadas, route tables y NSGs requeridos por Data Flow, Data Integration, ADB y Data Catalog
4. actualizar manifiestos de red en `workspace/oci-mirror/<env>/.../network/`
5. bloquear publish si faltan private endpoints requeridos
6. si la topologia o un recurso Terraform falla o no esta claro, usar `oci-terraform-fallback` para validar el recurso oficial antes de ajustar la red
