# oci-medallion-network-foundation

Usa esta skill para preparar la foundation de red del ambiente.

## Flujo

1. revisar `infra/stacks/<env>/main.tf`
2. confirmar VCN, subredes privadas y NSGs
3. actualizar manifiestos de red en `workspace/oci-mirror/<env>/.../network/`
4. bloquear publish si faltan private endpoints requeridos
