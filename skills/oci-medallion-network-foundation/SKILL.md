# oci-medallion-network-foundation

Usa esta skill para preparar la foundation de red del ambiente.

## Flujo

1. revisar `infra/stacks/<env>/main.tf`
2. confirmar VCN, subredes privadas y NSGs
3. actualizar manifiestos de red en `workspace/oci-mirror/<env>/.../network/`
4. bloquear publish si faltan private endpoints requeridos
5. si la topologia o un recurso Terraform falla o no esta claro, usar `oci-terraform-fallback` para validar el recurso oficial antes de ajustar la red
